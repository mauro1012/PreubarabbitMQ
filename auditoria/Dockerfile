# Usamos una imagen ligera de Node
FROM node:18-slim

# Directorio de trabajo
WORKDIR /app

# Copiamos dependencias
COPY package*.json ./
RUN npm install

# Copiamos el resto del código
COPY . .

# Comando de arranque
CMD ["npm", "start"]