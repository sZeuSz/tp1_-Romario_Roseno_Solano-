# ufam-db-tp1

Repositorio base para o Trabalho de Banco de Dados da Graduação em Ciencia da Computação na UFAM

## Copiando esse repositorio

Você deve ter uma conta no github, criar é gratis, e ele é essencial para a vida e carreira de você.

Para fazer isso siga esses passos:

https://user-images.githubusercontent.com/118348/229365938-48d261c8-b569-463c-bc00-462eb218b423.mp4

Para entender melhor [git e github](https://www.alura.com.br/artigos/o-que-e-git-github).

## Configurando

### Docker e Docker Compose

Instalando o [docker desktop e docker compose (Windows, Linux e Mac)](https://www.docker.com/products/docker-desktop/)

Instalando na linha de comando

[Docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04-pt) e [Docker Compose Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04-pt)

#### Como funciona o docker compose

[Docker Compose - Explicado](https://blog.4linux.com.br/docker-compose-explicado/)

### Postgres

Criar pasta `postgres-data` na raiz do projeto. Essa pasta **não deve ser enviada** para o github.

Depois você deve subir o docker-compose com o postgres. Da primeira vez vai demorar um pouco, e fique de olho nos logs para qualquer erro.

```bash
docker-compose up -d
```

### Python

Criar o ambiente virtual

```bash
python3 -m venv .tp1
```

Ativar o ambiente virtual

```bash
source .tp1/bin/activate
```

## Usando o postgres na sua maquina

Após subir, você conseguirá conectar no banco. Ele vem vazio e você terá que preencher ele com o que o trabalho pede.

```bash
psql -h localhost -U postgres
```

As credenciais são:

```yaml
username: postgres
password: postgres
```

## Para usar os scripts

Após criar seu banco de dados, preencha o arquivo .env.example com as respectivas informações e renomeie o arquivo para '.env'

## Usando Python

Para instalar bibliotecas necessarias para o trabalho, use o pip [DEPOIS de ativar o ambiente](#python) virtual.

```bash
pip install <biblioteca>
```
