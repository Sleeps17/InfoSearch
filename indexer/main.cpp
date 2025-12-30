#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <ctime>
#include <locale.h>
#include <wchar.h>
#include <wctype.h>

// Константы
const uint MAX_TOKEN_LEN = 64;
const uint MAX_DOCS = 1000000;
const uint HASH_SIZE = 1000003;
const uint MAX_LINE = 1024 * 1024;

// Структуры данных
struct DocNode {
    int doc_id;
    DocNode *next;
};

struct TermEntry {
    wchar_t term[MAX_TOKEN_LEN];
    long long freq;
    int doc_count;
    DocNode *docs;
    TermEntry *next;
};

struct Document {
    char title[256];
    char url[512];
    char oid[32];
};

typedef struct {
    int doc_count;
    long long total_tokens;
    long long total_token_length;
    long long total_input_bytes;
    long long total_unique_terms;
} Stats;

// Глобальные переменные
TermEntry *hash_table[HASH_SIZE];
Document documents[MAX_DOCS];
Stats stats = {0, 0, 0, 0, 0};

// Функция хеширования для wide char
unsigned int hash(const wchar_t *s) {
    unsigned int h = 5381;
    while (*s)
        h = ((h << 5) + h) + (unsigned int)(*s++);
    return h % HASH_SIZE;
}

// Фильтрация HTML
struct HtmlState {
    int inside_tag;
    int inside_script;
    int inside_style;
    int is_closing;
    char tag[32];
    int tag_len;
};

int html_filter_char(wchar_t c, HtmlState &st) {
    if (c == L'<') {
        st.inside_tag = 1;
        st.tag_len = 0;
        st.is_closing = 0;
        return 0;
    }

    if (st.inside_tag) {
        if (c == L'/')
            st.is_closing = 1;
        else if (iswalpha(c) && st.tag_len < 31) {
            st.tag[st.tag_len++] = (char)towlower(c);
            st.tag[st.tag_len] = 0;
        }

        if (c == L'>') {
            st.inside_tag = 0;
            if (!strcmp(st.tag, "script"))
                st.inside_script = !st.is_closing;
            else if (!strcmp(st.tag, "style"))
                st.inside_style = !st.is_closing;
        }
        return 0;
    }

    if (st.inside_script || st.inside_style)
        return 0;

    return c;
}

// Проверка на русскую или английскую букву
int is_valid_char(wchar_t c) {
    return iswalnum(c) ||
           (c >= L'А' && c <= L'Я') ||
           (c >= L'а' && c <= L'я') ||
           c == L'Ё' || c == L'ё';
}

// Стемминг для wide char
void stem(wchar_t *t) {
    int len = wcslen(t);
    if (len > 4 && !wcscmp(t + len - 2, L"ов")) t[len - 2] = 0;
    else if (len > 4 && !wcscmp(t + len - 2, L"ев")) t[len - 2] = 0;
    else if (len > 4 && !wcscmp(t + len - 2, L"ам")) t[len - 2] = 0;
    else if (len > 4 && !wcscmp(t + len - 2, L"ём")) t[len - 2] = 0;
    else if (len > 5 && !wcscmp(t + len - 3, L"ing")) t[len - 3] = 0;
    else if (len > 4 && !wcscmp(t + len - 2, L"ed")) t[len - 2] = 0;
}

// Построение индекса
void add_doc(TermEntry *t, int doc_id) {
    DocNode *n = t->docs;
    while (n) {
        if (n->doc_id == doc_id)
            return;
        n = n->next;
    }

    n = (DocNode *)malloc(sizeof(DocNode));
    n->doc_id = doc_id;
    n->next = t->docs;
    t->docs = n;
    t->doc_count++;
}

void add_term(const wchar_t *token, int doc_id) {
    unsigned int h = hash(token);
    TermEntry *e = hash_table[h];

    while (e) {
        if (!wcscmp(e->term, token)) {
            e->freq++;
            add_doc(e, doc_id);
            return;
        }
        e = e->next;
    }

    e = (TermEntry *)malloc(sizeof(TermEntry));
    wcscpy(e->term, token);
    e->freq = 1;
    e->doc_count = 0;
    e->docs = NULL;
    e->next = hash_table[h];
    hash_table[h] = e;
    stats.total_unique_terms++;

    add_doc(e, doc_id);
}

// Токенизация
void process_html(const char *html, int doc_id) {
    HtmlState st = {};
    wchar_t token[MAX_TOKEN_LEN];
    int len = 0;

    stats.total_input_bytes += strlen(html);

    // Конвертируем UTF-8 в wide char
    size_t html_len = strlen(html);
    wchar_t *whtml = (wchar_t *)malloc((html_len + 1) * sizeof(wchar_t));
    mbstowcs(whtml, html, html_len + 1);

    for (wchar_t *p = whtml; *p; p++) {
        wchar_t c = *p;

        if (c && is_valid_char(c)) {
            if (len < MAX_TOKEN_LEN - 1)
                token[len++] = towlower(c);
        } else {
            if (len > 0) {
                token[len] = 0;
                stem(token);
                add_term(token, doc_id);
                stats.total_tokens++;
                stats.total_token_length += len;
                len = 0;
            }
        }
    }

    free(whtml);
}

// Парсер JSON
char *extract(const char *json, const char *field, char *buffer, int buffer_size) {
    char search[256];
    snprintf(search, sizeof(search), "\"%s\":", field);
    const char *pos = strstr(json, search);
    if (!pos) return NULL;
    pos += strlen(search);
    while (*pos && isspace(*pos)) pos++;
    if (*pos == '"') {
        pos++;
        int i = 0;
        while (*pos && *pos != '"' && i < buffer_size - 1) {
            if (*pos == '\\' && *(pos + 1)) {
                pos++;
                if (*pos == 'n') buffer[i++] = '\n';
                else if (*pos == 't') buffer[i++] = '\t';
                else if (*pos == 'r') buffer[i++] = '\r';
                else buffer[i++] = *pos;
                pos++;
            } else {
                buffer[i++] = *pos++;
            }
        }
        buffer[i] = 0;
        return buffer;
    }
    return NULL;
}

// Сохранение индексов
void save_forward(const char *fn) {
    FILE *f = fopen(fn, "wb");
    fwrite(&stats.doc_count, sizeof(int), 1, f);

    for (int i = 0; i < stats.doc_count; i++) {
        int l;

        l = strlen(documents[i].title);
        fwrite(&l, sizeof(int), 1, f);
        fwrite(documents[i].title, 1, l, f);

        l = strlen(documents[i].url);
        fwrite(&l, sizeof(int), 1, f);
        fwrite(documents[i].url, 1, l, f);

        l = strlen(documents[i].oid);
        fwrite(&l, sizeof(int), 1, f);
        fwrite(documents[i].oid, 1, l, f);
    }
    fclose(f);
}

void save_inverted(const char *fn) {
    FILE *f = fopen(fn, "wb");
    fwrite(&stats.total_unique_terms, sizeof(long long), 1, f);

    for (int i = 0; i < HASH_SIZE; i++) {
        TermEntry *e = hash_table[i];
        while (e) {
            // Конвертируем wchar_t в UTF-8 для вывода
            char term_utf8[MAX_TOKEN_LEN * 4];
            wcstombs(term_utf8, e->term, sizeof(term_utf8));
            wprintf(L"TERM: %s\n", term_utf8);

            int l = wcslen(e->term);
            fwrite(&l, sizeof(int), 1, f);
            fwrite(e->term, sizeof(wchar_t), l, f);

            fwrite(&e->doc_count, sizeof(int), 1, f);

            DocNode *n = e->docs;
            while (n) {
                fwrite(&n->doc_id, sizeof(int), 1, f);
                n = n->next;
            }
            e = e->next;
        }
    }
    fclose(f);
}

int main() {
    // Устанавливаем локаль для работы с UTF-8
    setlocale(LC_ALL, "ru_RU.UTF-8");

    memset(hash_table, 0, sizeof(hash_table));

    char *line = NULL;
    size_t cap = 0;

    clock_t start = clock();

    while (getline(&line, &cap, stdin) > 0) {
        char html[MAX_LINE];
        if (!extract(line, "html_content", html, MAX_LINE))
            continue;

        extract(line, "$oid", documents[stats.doc_count].oid, 32);
        extract(line, "url", documents[stats.doc_count].url, 512);

        snprintf(documents[stats.doc_count].title, 256,
                 "Document %d", stats.doc_count);


        printf("URL: %s, HTML: %s\n", documents[stats.doc_count].url, html);

        process_html(html, stats.doc_count);
        stats.doc_count++;
    }

    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;

    save_forward("forward.idx");
    save_inverted("inverted.idx");

    fprintf(stderr, "Documents: %d\n", stats.doc_count);
    fprintf(stderr, "Unique terms: %lld\n", stats.total_unique_terms);
    fprintf(stderr, "Total tokens: %lld\n", stats.total_tokens);
    fprintf(stderr, "Avg token length: %.2f\n",
            (double)stats.total_token_length / stats.total_tokens);
    fprintf(stderr, "Input size: %.2f KB\n",
            stats.total_input_bytes / 1024.0);
    fprintf(stderr, "Time: %.3f sec\n", elapsed);
    fprintf(stderr, "Speed: %.2f KB/sec\n",
            (stats.total_input_bytes / 1024.0) / elapsed);

    free(line);
    return 0;
}
