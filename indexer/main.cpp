#include <iostream>
#include <ostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cctype>
#include <ctime>
#include <fstream>
#include <algorithm>

// Структуры данных
struct DocNode {
    int doc_id;
    DocNode* next;
};

struct TermEntry {
    std::string term;
    long long freq = 0;
    int doc_count = 0;
    DocNode* docs = nullptr;
};

struct Document {
    std::string title;
    std::string url;
    std::string oid;
};

struct Stats {
    int doc_count = 0;
    long long total_tokens = 0;
    long long total_token_length = 0;
    long long total_input_bytes = 0;
    long long total_unique_terms = 0;
};

// Глобальные переменные
std::unordered_map<std::string, TermEntry*> hash_table;
std::vector<Document> documents;
Stats stats;

bool is_valid_char(char c) {
    if (std::isalpha(static_cast<unsigned char>(c))) return true;
    unsigned char uc = static_cast<unsigned char>(c);
    return (uc >= 0xD0 && uc <= 0xD1);
}

// Стемминг
std::string stem(const std::string& token) {
    std::string t = token;
    if (t.size() > 2) {
        if (t.size() > 4 && t.substr(t.size()-2) == "ов") t = t.substr(0, t.size()-2);
        else if (t.size() > 4 && t.substr(t.size()-2) == "ев") t = t.substr(0, t.size()-2);
        else if (t.size() > 4 && t.substr(t.size()-2) == "ам") t = t.substr(0, t.size()-2);
        else if (t.size() > 4 && t.substr(t.size()-2) == "ём") t = t.substr(0, t.size()-2);
    }
    return t;
}

// Добавление документа к терму
void add_doc(TermEntry* t, int doc_id) {
    DocNode* n = t->docs;
    while (n) {
        if (n->doc_id == doc_id) return;
        n = n->next;
    }
    n = new DocNode{doc_id, t->docs};
    t->docs = n;
    t->doc_count++;
}

// Добавление терма
void add_term(const std::string& token, int doc_id) {
    auto it = hash_table.find(token);
    if (it != hash_table.end()) {
        it->second->freq++;
        add_doc(it->second, doc_id);
        return;
    }
    TermEntry* e = new TermEntry();
    e->term = token;
    e->freq = 1;
    e->doc_count = 0;
    e->docs = nullptr;
    add_doc(e, doc_id);
    hash_table[token] = e;
    stats.total_unique_terms++;
}

//Токенизация
void process_html(const std::string& html, int doc_id) {
    std::string token;
    stats.total_input_bytes += html.size();

    for (size_t i = 0; i < html.size(); ++i) {
        unsigned char c = html[i];
        if ((c & 0xC0) == 0x80) {
            token += c;
            continue;
        }

        if (is_valid_char(html[i])) {
            token += html[i];
        } else if (!token.empty()) {
            std::string t = stem(token);
            add_term(t, doc_id);
            stats.total_tokens++;
            stats.total_token_length += t.size();
            token.clear();
        }
    }
    if (!token.empty()) {
        std::string t = stem(token);
        add_term(t, doc_id);
        stats.total_tokens++;
        stats.total_token_length += t.size();
    }
}

// Парсер JSON
bool extract(const std::string& json, const std::string& field, std::string& out) {
    std::string search = "\"" + field + "\":\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return false;
    pos += search.size();

    size_t end = pos;
    while (end < json.size()) {
        if (json[end] == '\"' && (end == 0 || json[end-1] != '\\')) {
            break;
        }
        end++;
    }

    if (end == json.size()) return false;
    out = json.substr(pos, end - pos);

    size_t esc_pos = 0;
    while ((esc_pos = out.find("\\\"", esc_pos)) != std::string::npos) {
        out.replace(esc_pos, 2, "\"");
        esc_pos++;
    }

    return true;
}


// Сохранение прямого индекса
void save_forward(const char* fn) {
    std::ofstream out(fn, std::ios::binary);
    int doc_count = static_cast<int>(documents.size());
    out.write(reinterpret_cast<const char*>(&doc_count), sizeof(int));

    for (const auto& doc : documents) {
        int l = static_cast<int>(doc.title.size());
        out.write(reinterpret_cast<const char*>(&l), sizeof(int));
        out.write(doc.title.data(), l);

        l = static_cast<int>(doc.url.size());
        out.write(reinterpret_cast<const char*>(&l), sizeof(int));
        out.write(doc.url.data(), l);

        l = static_cast<int>(doc.oid.size());
        out.write(reinterpret_cast<const char*>(&l), sizeof(int));
        out.write(doc.oid.data(), l);
    }

    out.close();
}

// Сохранение обратного индекса
void save_inverted(const char* fn) {
    std::ofstream out(fn, std::ios::binary);
    long long total_terms = static_cast<long long>(hash_table.size());
    out.write(reinterpret_cast<const char*>(&total_terms), sizeof(long long));

    for (const auto& [term, entry] : hash_table) {
        out.write(reinterpret_cast<const char*>(&entry->freq), sizeof(long long));

        int l = static_cast<int>(entry->term.size());
        out.write(reinterpret_cast<const char*>(&l), sizeof(int));
        out.write(entry->term.data(), l);

        out.write(reinterpret_cast<const char*>(&entry->doc_count), sizeof(int));

        DocNode* n = entry->docs;
        while (n) {
            out.write(reinterpret_cast<const char*>(&n->doc_id), sizeof(int));
            n = n->next;
        }
    }

    out.close();
}

// Сохранение CSV для закона Ципфа
void save_zipf(const char* fn) {
    std::ofstream out(fn);
    out << "rank,term,frequency\n";

    std::vector<TermEntry*> terms;
    for (auto &p : hash_table) terms.push_back(p.second);

    std::sort(terms.begin(), terms.end(),
              [](TermEntry* a, TermEntry* b){ return b->freq < a->freq; });

    for (size_t i = 0; i < terms.size(); ++i) {
        out << (i+1) << "," << terms[i]->term << "," << terms[i]->freq << "\n";
    }
    out.close();
}



int main() {
    std::string line;
    clock_t start = clock();

    while (std::getline(std::cin, line)) {
        std::string html, url, oid;
        if (!extract(line, "html_content", html)) continue;
        extract(line, "$oid", oid);
        extract(line, "url", url);

        Document doc;
        doc.oid = oid;
        doc.url = url;
        doc.title = "Document " + std::to_string(stats.doc_count);
        documents.push_back(doc);

        std::cout << "\rProcessed document: " << stats.doc_count;

        process_html(html, stats.doc_count);
        stats.doc_count++;
    }

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    save_forward("forward.idx");
    save_inverted("inverted.idx");
    save_zipf("zipf.csv");


    // Вывод статистики
    std::cout << "Documents: " << stats.doc_count << "\n";
    std::cout << "Unique terms: " << stats.total_unique_terms << "\n";
    std::cout << "Total tokens: " << stats.total_tokens << "\n";
    std::cout << "Avg token length: "
              << (stats.total_tokens ? (double)stats.total_token_length / stats.total_tokens : 0) << "\n";
    std::cout << "Input size: " << stats.total_input_bytes / 1024.0 << " KB\n";
    std::cout << "Time: " << elapsed << " sec\n";
    std::cout << "Speed: " << (stats.total_input_bytes / 1024.0) / elapsed << " KB/sec\n";

    return 0;
}
