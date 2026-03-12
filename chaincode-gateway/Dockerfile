FROM node:22-alpine

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY package*.json ./
RUN npm install --production

COPY . .

ENV NODE_OPTIONS="--use-openssl-ca" \
    NODE_EXTRA_CA_CERTS="/workspace/data/fabric-tlscas.pem"

EXPOSE 3000

CMD ["npm", "start"]
