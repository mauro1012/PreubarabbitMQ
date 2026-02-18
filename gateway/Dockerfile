FROM node:18-slim

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

# El Gateway necesita el puerto 3000 para el ALB de AWS
EXPOSE 3000

CMD ["npm", "start"]