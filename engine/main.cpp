#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <cctype>
#include <unordered_set>

struct DocNode {
    int doc_id;
    DocNode* next;
};

struct TermEntry {
    std::string term;
    long long freq;
    int doc_count;
    DocNode* docs;

    ~TermEntry() {
        DocNode* n = docs;
        while (n) {
            DocNode* next = n->next;
            delete n;
            n = next;
        }
    }
};

std::vector<std::string> documents;
std::unordered_map<std::string, TermEntry*> hash_table;

void load_forward(const char* fn) {
    std::ifstream in(fn, std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open forward index file\n";
        return;
    }

    int doc_count;
    in.read(reinterpret_cast<char*>(&doc_count), sizeof(int));
    documents.resize(doc_count);

    for (int i = 0; i < doc_count; ++i) {
        int l;
        std::string title, url, oid;

        // title
        in.read(reinterpret_cast<char*>(&l), sizeof(int));
        title.resize(l);
        in.read(title.data(), l);

        // url
        in.read(reinterpret_cast<char*>(&l), sizeof(int));
        url.resize(l);
        in.read(url.data(), l);

        // oid
        in.read(reinterpret_cast<char*>(&l), sizeof(int));
        oid.resize(l);
        in.read(oid.data(), l);

        documents[i] = url;
    }

    in.close();
}

void load_inverted(const char* fn) {
    std::ifstream in(fn, std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open inverted index file\n";
        return;
    }

    long long total_terms;
    in.read(reinterpret_cast<char*>(&total_terms), sizeof(long long));

    for (long long i = 0; i < total_terms; ++i) {
        TermEntry* e = new TermEntry();

        in.read(reinterpret_cast<char*>(&e->freq), sizeof(long long));

        int l;
        in.read(reinterpret_cast<char*>(&l), sizeof(int));
        e->term.resize(l);
        in.read(e->term.data(), l);

        in.read(reinterpret_cast<char*>(&e->doc_count), sizeof(int));

        e->docs = nullptr;
        DocNode* last = nullptr;
        for (int j = 0; j < e->doc_count; ++j) {
            int doc_id;
            in.read(reinterpret_cast<char*>(&doc_id), sizeof(int));
            DocNode* node = new DocNode{doc_id, nullptr};
            if (!e->docs) e->docs = node;
            if (last) last->next = node;
            last = node;
        }

        hash_table[e->term] = e;
    }

    in.close();
}

using DocList = std::unordered_set<int>;

DocList get_docs_for_term(const std::string& term) {
    DocList result;
    auto it = hash_table.find(term);
    if (it != hash_table.end()) {
        DocNode* n = it->second->docs;
        while (n) {
            result.insert(n->doc_id);
            n = n->next;
        }
    }
    return result;
}

DocList intersect(const DocList& a, const DocList& b) {
    DocList result;
    if (a.size() > b.size()) {
        for (int doc_id : b) {
            if (a.find(doc_id) != a.end()) {
                result.insert(doc_id);
            }
        }
    } else {
        for (int doc_id : a) {
            if (b.find(doc_id) != b.end()) {
                result.insert(doc_id);
            }
        }
    }
    return result;
}

DocList union_op(const DocList& a, const DocList& b) {
    DocList result = a;
    result.insert(b.begin(), b.end());
    return result;
}

DocList complement(const DocList& a) {
    DocList result;
    for (int i = 0; i < documents.size(); ++i) {
        if (a.find(i) == a.end()) {
            result.insert(i);
        }
    }
    return result;
}

enum TokenType {
    TOKEN_TERM,
    TOKEN_AND,
    TOKEN_OR,
    TOKEN_NOT,
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_END
};

struct Token {
    TokenType type;
    std::string value;

    Token() : type(TOKEN_END), value("") {}

    Token(TokenType t, const std::string& v = "") : type(t), value(v) {}
};

class QueryParser {
private:
    std::string query_;
    size_t pos_;

    void skip_whitespace() {
        while (pos_ < query_.size() && std::isspace(query_[pos_])) {
            ++pos_;
        }
    }

    std::string read_term() {
        size_t start = pos_;
        while (pos_ < query_.size()) {
            char c = query_[pos_];
            if (std::isspace(c) || c == '(' || c == ')' ||
                c == '!' || c == '&' || c == '|') {
                break;
            }
            ++pos_;
        }
        return query_.substr(start, pos_ - start);
    }

public:
    QueryParser(const std::string& query) : query_(query), pos_(0) {}

    Token get_next_token() {
        skip_whitespace();

        if (pos_ >= query_.size()) {
            return Token(TOKEN_END);
        }

        char c = query_[pos_];

        switch (c) {
            case '(':
                ++pos_;
                return Token(TOKEN_LPAREN);
            case ')':
                ++pos_;
                return Token(TOKEN_RPAREN);
            case '!':
                ++pos_;
                return Token(TOKEN_NOT);
            case '&':
                if (pos_ + 1 < query_.size() && query_[pos_ + 1] == '&') {
                    pos_ += 2;
                    return Token(TOKEN_AND);
                }
                break;
            case '|':
                if (pos_ + 1 < query_.size() && query_[pos_ + 1] == '|') {
                    pos_ += 2;
                    return Token(TOKEN_OR);
                }
                break;
        }

        std::string term = read_term();
        if (!term.empty()) {
            return Token(TOKEN_TERM, term);
        }

        ++pos_;
        return get_next_token();
    }
};

class QueryEvaluator {
private:
    QueryParser& parser_;
    Token current_token_;

    void advance() {
        current_token_ = parser_.get_next_token();
    }

    DocList parse_expression() {
        DocList result = parse_term();

        while (current_token_.type == TOKEN_AND || current_token_.type == TOKEN_OR) {
            TokenType op = current_token_.type;
            advance();
            DocList right = parse_term();

            if (op == TOKEN_AND) {
                result = intersect(result, right);
            } else { // TOKEN_OR
                result = union_op(result, right);
            }
        }

        return result;
    }

    DocList parse_term() {
        if (current_token_.type == TOKEN_NOT) {
            advance();
            DocList result = parse_factor();
            return complement(result);
        }

        return parse_factor();
    }

    DocList parse_factor() {
        if (current_token_.type == TOKEN_LPAREN) {
            advance();
            DocList result = parse_expression();
            if (current_token_.type != TOKEN_RPAREN) {
                std::cerr << "Error: Expected ')'\n";
                return DocList();
            }
            advance();
            return result;
        }

        if (current_token_.type == TOKEN_TERM) {
            std::string term = current_token_.value;
            advance();
            return get_docs_for_term(term);
        }

        std::cerr << "Error: Unexpected token\n";
        return DocList();
    }

public:
    QueryEvaluator(QueryParser& parser) : parser_(parser) {
        advance();
    }

    DocList evaluate() {
        return parse_expression();
    }
};

void search_single_term(const std::string& term) {
    auto it = hash_table.find(term);
    if (it == hash_table.end()) {
        std::cout << "Term not found\n";
        return;
    }

    TermEntry* e = it->second;
    std::cout << "Term: " << e->term << ", freq=" << e->freq
              << ", doc_count=" << e->doc_count << "\nDocuments:\n";

    DocNode* n = e->docs;
    int count = 0;
    while (n && count < 50) {
        if (n->doc_id >= 0 && n->doc_id < documents.size()) {
            std::cout << "- " << documents[n->doc_id] << "\n";
            count++;
        }
        n = n->next;
    }

    if (e->doc_count > 50) {
        std::cout << "... and " << (e->doc_count - 50) << " more documents\n";
    }
}

void search_boolean(const std::string& query) {
    QueryParser parser(query);
    QueryEvaluator evaluator(parser);
    DocList result = evaluator.evaluate();

    std::cout << "Found " << result.size() << " documents:\n";

    std::vector<int> sorted_docs(result.begin(), result.end());
    std::sort(sorted_docs.begin(), sorted_docs.end());

    int count = 0;
    for (int doc_id : sorted_docs) {
        if (count >= 50) {
            std::cout << "... and " << (result.size() - 50) << " more documents\n";
            break;
        }
        if (doc_id >= 0 && doc_id < documents.size()) {
            std::cout << "- " << documents[doc_id] << "\n";
            count++;
        }
    }
}

int main(int argc, char* argv[]) {
    // Загружаем индексы
    load_forward("forward.idx");
    load_inverted("inverted.idx");

    if (argc > 1) {
        std::string query;

        for (int i = 1; i < argc; i++) {
            if (i > 1) query += " ";
            query += argv[i];
        }

        // Определяем тип запроса
        bool is_simple = true;
        for (char c : query) {
            if (c == '&' || c == '|' || c == '!' || c == '(' || c == ')') {
                is_simple = false;
                break;
            }
        }

        if (is_simple) {
            // Простой однотермовый запрос
            search_single_term(query);
        } else {
            // Булев запрос
            search_boolean(query);
        }
    } else {
        std::cout << "Search engine loaded.\n";
        std::cout << "Documents: " << documents.size() << "\n";
        std::cout << "Unique terms: " << hash_table.size() << "\n\n";

        std::cout << "Usage:\n";
        std::cout << "  - Single term: матч\n";
        std::cout << "  - AND operation: матч && футбол\n";
        std::cout << "  - OR operation: матч || игра\n";
        std::cout << "  - NOT operation: !теннис\n";
        std::cout << "  - Parentheses: (красный || желтый) автомобиль\n";
        std::cout << "  - Complex: матч && (футбол || хоккей) && !теннис\n";
        std::cout << "  - Multiple spaces are allowed\n\n";

        std::string query;
        std::cout << "Enter search query (empty to exit): ";

        while (std::getline(std::cin, query)) {
            if (query.empty()) break;

            bool is_simple = true;
            for (char c : query) {
                if (c == '&' || c == '|' || c == '!' || c == '(' || c == ')') {
                    is_simple = false;
                    break;
                }
            }

            if (is_simple) {
                search_single_term(query);
            } else {
                search_boolean(query);
            }

            std::cout << "\nEnter search query (empty to exit): ";
        }
    }

    // Очистка памяти
    for (auto& entry : hash_table) {
        delete entry.second;
    }

    return 0;
}
