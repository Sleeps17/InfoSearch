import gzip
import hashlib
import logging
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
import yaml
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WebCrawler:
    def __init__(self, config_path):
        self.config = self._load_config(config_path)
        self.db = self._init_database()
        self.delay = self.config["logic"]["delay"]
        self.sources = self.config["logic"].get("sources", [])
        self.recheck_interval = self.config["logic"].get("recheck_interval", 86400)
        self.user_agent = self.config["logic"].get("user_agent", "SearchBot/1.0")
        self.respect_robots_txt = self.config["logic"].get("respect_robots_txt", True)
        self.visited_urls = set()
        self.queue = []
        self.robots_parsers = {}
        self.count_map = {}
        self.limit = 15000

    def _load_config(self, config_path):
        """Загрузка конфигурации из YAML файла"""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            logger.info(f"Конфигурация загружена из {config_path}")
            return config
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
            sys.exit(1)

    def _init_database(self):
        """Инициализация подключения к MongoDB"""
        try:
            db_config = self.config["db"]
            client = MongoClient(
                host=db_config.get("host", "localhost"),
                port=db_config.get("port", 27017),
                username=db_config.get("username"),
                password=db_config.get("password"),
            )
            db = client[db_config.get("database", "crawler_db")]

            # Создаем индексы для оптимизации
            db.documents.create_index("url", unique=True)
            db.documents.create_index("crawl_date")
            db.documents.create_index("source_name")
            db.queue.create_index("url", unique=True)
            db.queue.create_index("source_name")

            logger.info("Подключение к MongoDB установлено")
            return db
        except Exception as e:
            logger.error(f"Ошибка подключения к MongoDB: {e}")
            sys.exit(1)

    def normalize_url(self, url):
        """Нормализация URL"""
        parsed = urlparse(url)
        normalized = urlunparse(
            (
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                parsed.path.rstrip("/") if parsed.path != "/" else "/",
                parsed.params,
                parsed.query,
                "",
            )
        )
        return normalized

    def calculate_hash(self, html_content):
        """Вычисление хеша HTML содержимого"""
        return hashlib.md5(html_content.encode("utf-8")).hexdigest()

    def get_robots_parser(self, base_url):
        """Получение или создание парсера robots.txt для домена"""
        parsed = urlparse(base_url)
        domain = f"{parsed.scheme}://{parsed.netloc}"

        if domain not in self.robots_parsers:
            robots_url = urljoin(domain, "/robots.txt")
            rp = RobotFileParser()
            rp.set_url(robots_url)

            try:
                rp.read()
                logger.info(f"Загружен robots.txt для {domain}")

                # Получаем crawl-delay если есть
                crawl_delay = rp.crawl_delay(self.user_agent)
                if crawl_delay:
                    logger.info(f"Crawl-delay для {domain}: {crawl_delay} сек")
                    self.delay = max(self.delay, crawl_delay)

            except Exception as e:
                logger.warning(f"Ошибка загрузки robots.txt для {domain}: {e}")

            self.robots_parsers[domain] = rp

        return self.robots_parsers[domain]

    def can_fetch(self, url):
        """Проверка, разрешено ли обкачивать URL согласно robots.txt"""
        # Если отключено соблюдение robots.txt, разрешаем всё
        if not self.respect_robots_txt:
            return True

        try:
            rp = self.get_robots_parser(url)
            can_fetch = rp.can_fetch(self.user_agent, url)

            if not can_fetch:
                logger.debug(f"URL запрещен robots.txt: {url}")

            return can_fetch
        except Exception as e:
            logger.warning(f"Ошибка проверки robots.txt для {url}: {e}")
            # В случае ошибки разрешаем обкачку
            return True

    def parse_sitemap(self, sitemap_url, source_name):
        """Парсинг sitemap.xml и добавление URL в очередь"""
        try:
            logger.info(f"Парсинг sitemap: {sitemap_url}")

            headers = {"User-Agent": self.user_agent}
            response = requests.get(sitemap_url, headers=headers, timeout=10)
            response.raise_for_status()

            # Проверяем, не gzip ли это
            content = response.content
            if sitemap_url.endswith(".gz"):
                content = gzip.decompress(content)

            # Парсим XML
            root = ET.fromstring(content)

            # Убираем namespace из тега для простоты
            root_tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

            urls_added = 0

            # Проверяем тип sitemap по корневому тегу
            if root_tag == "sitemapindex":
                # Это sitemap index - парсим вложенные sitemap
                sitemaps = root.findall(".//{*}sitemap")
                logger.info(
                    f"Найден sitemap index с {len(sitemaps)} вложенными sitemap"
                )

                for sitemap in sitemaps:
                    loc = sitemap.find("{*}loc")
                    if loc is not None and loc.text:
                        self.parse_sitemap(loc.text, source_name)

            elif root_tag == "urlset":
                # Это обычный sitemap с URL'ами
                urls = root.findall(".//{*}url")
                logger.info(f"Найдено {len(urls)} URL в sitemap")

                for url_elem in urls:
                    loc = url_elem.find("{*}loc")

                    if loc is not None and loc.text:
                        url = self.normalize_url(loc.text)

                        # Проверяем robots.txt
                        if self.can_fetch(url) or True:
                            # Получаем lastmod если есть
                            lastmod = url_elem.find("{*}lastmod")

                            lastmod_date = None
                            if lastmod is not None and lastmod.text:
                                try:
                                    lastmod_date = datetime.fromisoformat(
                                        lastmod.text.replace("Z", "+00:00")
                                    ).timestamp()
                                except:
                                    pass

                            if self.count_map.get(source_name, 0) >= self.limit:
                                return

                            self.queue.append(
                                {
                                    "url": url,
                                    "source_name": source_name,
                                    "lastmod": lastmod_date,
                                }
                            )
                            urls_added += 1
                            self.count_map[source_name] = (
                                self.count_map.get(source_name, 0) + 1
                            )

                if urls_added == 0 and len(urls) > 0:
                    logger.warning(
                        f"Все {len(urls)} URL из sitemap заблокированы robots.txt!"
                    )
                    logger.warning(
                        f"Проверьте robots.txt для этого домена или установите respect_robots_txt: false в конфиге"
                    )

                logger.info(f"Добавлено {urls_added} URL из sitemap")
            else:
                logger.warning(f"Неизвестный тип sitemap: корневой тег '{root_tag}'")

        except ET.ParseError as e:
            logger.error(f"Ошибка парсинга XML sitemap {sitemap_url}: {e}")
        except requests.RequestException as e:
            logger.error(f"Ошибка загрузки sitemap {sitemap_url}: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при парсинге sitemap {sitemap_url}: {e}")

    def discover_sitemaps(self, base_url, source_name):
        """Обнаружение sitemap через robots.txt и стандартные пути"""
        sitemaps = []

        # 1. Пытаемся найти sitemap в robots.txt
        try:
            rp = self.get_robots_parser(base_url)
            if hasattr(rp, "site_maps") and rp.site_maps():
                for sitemap_url in rp.site_maps():
                    sitemaps.append(sitemap_url)
                    logger.info(f"Sitemap найден в robots.txt: {sitemap_url}")
        except Exception as e:
            logger.warning(f"Ошибка чтения sitemap из robots.txt: {e}")

        # 2. Проверяем стандартные пути
        standard_paths = [
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemap.xml.gz",
            "/sitemap/sitemap.xml",
        ]

        parsed = urlparse(base_url)
        domain = f"{parsed.scheme}://{parsed.netloc}"

        for path in standard_paths:
            sitemap_url = urljoin(domain, path)
            try:
                headers = {"User-Agent": self.user_agent}
                response = requests.head(sitemap_url, headers=headers, timeout=5)
                if response.status_code == 200:
                    if sitemap_url not in sitemaps:
                        sitemaps.append(sitemap_url)
                        logger.info(
                            f"Sitemap найден по стандартному пути: {sitemap_url}"
                        )
            except:
                pass

        # 3. Парсим все найденные sitemap'ы
        if sitemaps:
            for sitemap_url in sitemaps:
                self.parse_sitemap(sitemap_url, source_name)
        else:
            logger.warning(f"Sitemap не найден для {base_url}")
            # Добавляем сам base_url в очередь как fallback
            self.queue.append(
                {
                    "url": self.normalize_url(base_url),
                    "source_name": source_name,
                    "lastmod": None,
                }
            )

    def fetch_page(self, url):
        """Загрузка страницы по URL"""
        try:
            headers = {"User-Agent": self.user_agent}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Ошибка при загрузке {url}: {e}")
            return None

    def should_recrawl(self, url, lastmod=None):
        """Проверка, нужно ли переобкачать документ"""
        doc = self.db.documents.find_one({"url": url})
        if not doc:
            return True

        current_time = int(time.time())
        last_crawl = doc.get("crawl_date", 0)

        # Если есть lastmod из sitemap, используем его
        if lastmod and lastmod > last_crawl:
            logger.info(f"Документ изменен согласно sitemap lastmod: {url}")
            return True

        # Иначе проверяем по интервалу
        return (current_time - last_crawl) > self.recheck_interval

    def save_document(self, url, html_content, source_name):
        """Сохранение документа в базу данных"""
        normalized_url = self.normalize_url(url)
        content_hash = self.calculate_hash(html_content)
        current_time = int(time.time())

        # Проверяем, изменился ли документ
        existing_doc = self.db.documents.find_one({"url": normalized_url})

        if existing_doc:
            old_hash = existing_doc.get("content_hash", "")
            if old_hash == content_hash:
                logger.info(f"Документ не изменился: {normalized_url}")
                # Обновляем только дату проверки
                self.db.documents.update_one(
                    {"url": normalized_url}, {"$set": {"last_check_date": current_time}}
                )
                return False
            else:
                logger.info(f"Документ изменился, обновляем: {normalized_url}")

        # Сохраняем или обновляем документ
        document = {
            "url": normalized_url,
            "html_content": html_content,
            "source_name": source_name,
            "crawl_date": current_time,
            "last_check_date": current_time,
            "content_hash": content_hash,
        }

        self.db.documents.update_one(
            {"url": normalized_url}, {"$set": document}, upsert=True
        )

        logger.info(f"Документ сохранен: {normalized_url}")
        return True

    def save_queue_state(self):
        """Сохранение состояния очереди в БД"""
        try:
            # Удаляем старую очередь
            self.db.queue.delete_many({})

            # Сохраняем текущую очередь
            if self.queue:
                self.db.queue.insert_many(self.queue)

            logger.info(f"Состояние очереди сохранено ({len(self.queue)} URLs)")
        except Exception as e:
            logger.error(f"Ошибка сохранения очереди: {e}")

    def load_queue_state(self):
        """Загрузка состояния очереди из БД"""
        try:
            queue_docs = list(self.db.queue.find())
            if queue_docs:
                self.queue = queue_docs
                logger.info(f"Очередь восстановлена ({len(self.queue)} URLs)")
                return True
            else:
                logger.info("Очередь пуста, начинаем с источников из конфига")
                return False
        except Exception as e:
            logger.error(f"Ошибка загрузки очереди: {e}")
            return False

    def initialize_sources(self):
        """Инициализация источников данных из конфига"""
        logger.info(f"Инициализация {len(self.sources)} источников")

        for source in self.sources:
            source_name = source.get("name", "unknown")
            base_url = source.get("url")

            if not base_url:
                logger.warning(f"Источник {source_name} не имеет URL, пропускаем")
                continue

            logger.info(f"Обработка источника: {source_name} ({base_url})")

            # Обнаруживаем и парсим sitemap'ы
            self.discover_sitemaps(base_url, source_name)

        logger.info(f"Инициализация завершена. В очереди {len(self.queue)} URL")

    def crawl(self):
        """Основной цикл обкачки"""
        logger.info("Запуск поискового робота")

        # Пытаемся загрузить состояние очереди
        if not self.load_queue_state():
            # Если очередь пуста, инициализируем из источников
            self.initialize_sources()
            self.save_queue_state()

        if not self.queue:
            logger.error("Очередь пуста! Нечего обкачивать.")
            return

        try:
            processed_count = 0

            while self.queue:
                item = self.queue.pop(0)
                url = item["url"]
                source_name = item["source_name"]
                lastmod = item.get("lastmod")

                # Проверяем, не обрабатывали ли уже
                if url in self.visited_urls:
                    continue

                # Проверяем, нужно ли обкачивать
                if not self.should_recrawl(url, lastmod):
                    logger.info(f"Пропускаем (недавно обкачан): {url}")
                    self.visited_urls.add(url)
                    continue

                # Проверяем robots.txt
                # if not self.can_fetch(url):
                #     logger.warning(f"Запрещено robots.txt: {url}")
                #     self.visited_urls.add(url)
                #     continue

                logger.info(f"Обкачка [{processed_count + 1}] [{source_name}]: {url}")

                # Загружаем страницу
                html_content = self.fetch_page(url)

                if html_content:
                    # Сохраняем документ
                    self.save_document(url, html_content, source_name)
                    self.visited_urls.add(url)
                    processed_count += 1

                # Задержка между запросами
                time.sleep(self.delay)

                # Периодически сохраняем состояние
                if processed_count % 10 == 0:
                    self.save_queue_state()
                    logger.info(
                        f"Прогресс: обработано {processed_count} документов, "
                        f"осталось {len(self.queue)} в очереди"
                    )

        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки")
            self.save_queue_state()
            logger.info("Состояние сохранено. Робот остановлен.")

        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
            self.save_queue_state()

        finally:
            logger.info(f"Обкачано документов: {len(self.visited_urls)}")
            self.save_queue_state()


def main():
    """Точка входа в программу"""
    if len(sys.argv) != 2:
        print("Использование: python crawler.py config.yaml")
        sys.exit(1)

    config_path = sys.argv[1]
    crawler = WebCrawler(config_path)
    crawler.crawl()


if __name__ == "__main__":
    main()
