FROM alpine:3.19 AS builder

RUN apk add --no-cache g++ make

WORKDIR /app
COPY main.cpp .

RUN g++ main.cpp -o indexer


FROM alpine:3.19

RUN apk add --no-cache bash mongodb-tools

# Копируем скомпилированную программу
COPY --from=builder /app/indexer /usr/local/bin/

# Команда по умолчанию - запуск индексера
CMD ["sh", "-c", "mongoexport --host=$MONGO_HOST:$MONGO_PORT --db=$MONGO_DB --collection=$MONGO_COLLECTION --username=$MONGO_USER --password=$MONGO_PASSWORD --authenticationDatabase=$AUTH_DB --fields=_id,html_content,url --quiet | indexer"]
