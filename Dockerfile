FROM node:14 AS build

WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . ./
RUN npm run build

FROM node:15
ENV NODE_ENV='production'

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm install
COPY --from=build /src/tsoutput/* ./

EXPOSE 5433
CMD ["npm", "run", "start:built"]
