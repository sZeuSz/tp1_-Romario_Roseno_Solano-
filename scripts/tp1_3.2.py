import csv
import psycopg2
from dotenv import load_dotenv
from itertools import islice
import datetime
import os
import re

load_dotenv()

# Função para limpar todas as tabelas do banco de dados
def reset_database(conn):
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS product_similars')
    cur.execute('DROP TABLE IF EXISTS product_reviews')
    cur.execute('DROP TABLE IF EXISTS reviews')
    cur.execute('DROP TABLE IF EXISTS customers')
    cur.execute('DROP TABLE IF EXISTS product_categories')
    cur.execute('DROP TABLE IF EXISTS categories')
    cur.execute('DROP TABLE IF EXISTS products')
    conn.commit()

# Abrir arquivo e ler todas as linhas
with open("amazon-meta.txt", "r") as f:

    # Conexão com o banco de dados
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

    reset_database(conn)

    # Criação da tabela 'products'
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            asin TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            salesrank INTEGER NOT NULL,
            group_name TEXT NOT NULL
        )
    """)
    conn.commit()

    # Criação da tabela 'categories'
    cur.execute("""
        CREATE TABLE categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NULL,
            parent_id INTEGER NULL REFERENCES categories(id)
        )
    """)
    conn.commit()

    # Criação da tabela 'product_categories'
    cur.execute("""
        CREATE TABLE product_categories (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES products(id),
            category_id INTEGER NOT NULL REFERENCES categories(id)
        )
    """)
    conn.commit()

    # Criação da tabela 'customers'
    cur.execute("""
        CREATE TABLE customers (
            cid TEXT PRIMARY KEY
        )
    """)
    conn.commit()

    # Criação da tabela 'reviews'
    cur.execute("""
        CREATE TABLE reviews (
            id SERIAL PRIMARY KEY,
            customer_cid TEXT REFERENCES customers(cid),
            product_id INTEGER REFERENCES products(id),
            rating INTEGER NOT NULL,
            votes INTEGER NOT NULL,
            helpful INTEGER NOT NULL,
            date DATE NOT NULL
        )
    """)
    conn.commit()

    # Criação da tabela 'product_reviews'
    cur.execute("""
        CREATE TABLE product_reviews (
            product_id INTEGER REFERENCES products(id),
            review_id INTEGER REFERENCES reviews(id),
            PRIMARY KEY (product_id, review_id)
        )
    """)
    conn.commit()

    # Criação da tabela 'product_similars'
    cur.execute("""
        CREATE TABLE product_similars (
            product_asin TEXT REFERENCES products(asin),
            similar_product_asin TEXT,
            PRIMARY KEY (product_asin, similar_product_asin)
        )
    """)
    conn.commit()





    # Criação de um cursor para realizar operações no banco de dados
    cur = conn.cursor()

    lines = f.readlines()

    # Objeto para guardar todas as informações do objeto em questão
    product = {}

    # Loop pelas linhas do arquivo
    for line in lines:
        # Se é uma linha indiferente ou produto descontinuado, ignora e continua
        if("discontinued product" in line or "Total items" in line or "Full information about Amazon" in line):
            print("Produto descontinuado ou linha indiferente, não entra no banco de dados")
            product = {}
        # Se a linha começa com "Id: ", criar um novo dicionário para o produto
        elif(line.startswith('Id:   ')):
            # Se não tem nada no dicionário, ignora
            if(not len(product)):
                product = {}
            # Caso contrário, pega o próximo id e usa com -1 para acertar o index e adiciona o produto
            else:
                product["id"] = line[5:].strip()
                print("Inserindo produto...")

                # Insere novo produto com as informações necessárias
                cur.execute("INSERT INTO products (id, asin, title, salesrank, group_name) VALUES (%s, %s, %s, %s, %s) RETURNING id",(int(product['id']) - 1, product['ASIN'], product['title'],product['salesrank'], product['group']))
                # Pega o id retorno da inserção para usar posteriormente
                id_inserido = cur.fetchone()[0]
                # Se existe a key 'similars' no dicionário, insere os similares do produto
                if('similars' in product):
                    # Loop para percorrer os similares
                    for similar in product['similars']:
                        # Insere os similares do produto usando o asin de ambos
                        cur.execute("INSERT INTO product_similars (product_asin, similar_product_asin) VALUES (%s, %s)",(product["ASIN"],similar,))
                # Se existe a key 'category' no dicionário, insere o último id da hierarquia de categorias do produto
                if('category' in product):
                    # Insere o id da categoria mais específica
                    cur.execute("INSERT INTO product_categories (product_id, category_id) VALUES (%s, %s)", (int(product['id']) - 1, product['category'][0]))
                # Se existe a key 'reviews' no dicionário, insere as reviews devidamente
                if('reviews' in product):
                    # Loop para percorrer as reviews
                    for review in product['reviews']:
                        id_customer = None
                        # Verifica se já existe esse customer
                        cur.execute("SELECT cid FROM customers WHERE cid=%s", (review['customer'],))
                        row = cur.fetchone()
                        # Se já existir esse customer na tabela, uso o id dela
                        if row:
                            id_customer = row[0]
                        # Caso contrário, insiro esse novo customer na tabela de customers
                        else:
                            # Insere o novo customer na tabela customers
                            cur.execute("INSERT INTO customers (cid) VALUES (%s) ON CONFLICT DO NOTHING RETURNING cid;", (review['customer'],))
                            # Atribue o id resultado da inserção do customers para usar posteriormente
                            id_customer = cur.fetchone()[0]
                        #Insere review completa usando todas as informações necessárias
                        cur.execute("INSERT INTO reviews (customer_cid, product_id, rating, votes, helpful, date) VALUES (%s, %s, %s, %s, %s, %s)",(id_customer, int(product["id"]) - 1, review['rating'],review['votes'], review['helpful'], review['date']))
                product = {}
                conn.commit()
                
        # Se a linha começa com 'ASIN:', adicionar o ASIN ao dicionário do produto
        elif(line.startswith('ASIN:')):
            product["ASIN"] = line[6:].strip()
        # Se a linha começa com '  title:', adicionar o título ao dicionário do produto
        elif(line.startswith('  title:')):
            product["title"] = line[9:].strip()
        # Se a linha começa com '  group:', adicionar o grupo ao dicionário do produto
        elif(line.startswith('  group:')):
            product["group"] = line[9:].strip()
        # Se a linha começa com '  salesrank:', adicionar o salesrank ao dicionário do produto
        elif(line.startswith('  salesrank:')):
            product["salesrank"] = line[13:].strip()
        # Se a linha começa com '  similar:', adicionar a lista de ASINs similares ao dicionário do produto
        elif(line.startswith('  similar:')):
            asin_pattern = r'\b[A-Z0-9]{10}\b'
            matches = re.findall(asin_pattern, line)
            if matches:
                product["similars"] = matches
        # Se a linha começa com "  categories:", adicionar a lista de categorias ao dicionário do produto
        elif(line.startswith('  categories:')):
            categories = []
            # Loop pelas linhas de categorias e adicionar cada uma à lista de categorias
            for cat_line in lines[lines.index(line)+1:]:
                if cat_line.startswith("   |"):
                    categories.append(cat_line.strip())
                else:
                    break
            # Se tiver categorias para processar
            if(len(categories)):
                parent_id = None
                # Loop array de categorias (que são linhas)
                for category in categories:
                    # Separando as categorias uma da outra
                    parts = category.strip("|").split("|")
                    # Loop para passar em cada categoria separadamente (part)
                    for part in parts:
                        # Usa regex para pegar o nome da categoria (nome[idCategoria])
                        name_category = re.findall(r'(\w+)\[\d+\]', part)
                        # Usa regex para pegar o id da categoria (nome[idCategoria])
                        id_category = re.findall(r'\[(\d+)\]', part)
                        # Se tiver nome e id (porque tem categorias só com ID [idCategoria])
                        if name_category and id_category:
                            # Busca se categoria já estive
                            cur.execute("SELECT id FROM categories WHERE id = %s", (int(id_category[0]),))
                            row = cur.fetchone()
                            # Se a categoria existir salvo o id dela para usar posteriormente
                            if row:
                                parent_id = row[0]
                            # Caso contrário, insiro a nova categoria retornando o id da inserção
                            else:
                                # Insere categoria na tabela de categorias
                                cur.execute("INSERT INTO categories (id, name, parent_id) VALUES (%s, %s, %s) RETURNING id", (int(id_category[0]), name_category[0], None))
                                # Salva o parent_id com o resultado da inserção
                                parent_id = cur.fetchone()[0]
                        # Caso contrário se tiver só id ca categoria, insiro sem o nome
                        elif id_category:
                            # Busca se a categoria já estive
                            cur.execute("SELECT id FROM categories WHERE id = %s", (int(id_category[0]),))
                            row = cur.fetchone()
                            # Se já existir, salvo o id para usar posteriormente
                            if row:
                                # Salvando id para usar posteriormente
                                parent_id = row[0]
                            # Caso contrário, insiro a nova categoria retornando o id da inserção
                            else:
                                # Insere a categoria na tabela de categorias retornando o id ao finalizar a inserção
                                cur.execute("INSERT INTO categories (id, name, parent_id) VALUES (%s, %s, %s) RETURNING id", (int(id_category[0]), None,None))
                                # Salva o id que é retorno da inserção
                                parent_id = cur.fetchone()[0]
                        # Atualizar o parent_id para o último id da categoria da hierarquia
                        parent_id = id_category
                # Salva informação do parent_id no produto
                product['category'] = parent_id
                conn.commit()
        # Se a linha começa com '  reviews:'
        elif(line.startswith('  reviews:')):
            # Array para salvar as N próximas reviews
            array_reviews = []
            # Array para salvar todas as reviews extraídas
            product["reviews"] = []
            # Regex para pegar a quantidade de reviews baixados
            amount_downloaded_reviews = re.findall(r'reviews:.+?downloaded: (\d+)', line)
            # Adiciona as N próximas linhas baseado no amount_downloaded
            for review_line in (lines[lines.index(line)+1 : lines.index(line) + 1 + int(amount_downloaded_reviews[0])]):
                array_reviews.append(review_line.strip())
            # Loop de array de reviews (Que são linhas)
            for review in array_reviews:
                # Extraindo data da linha
                data = review.split()[0]
                # Extraindo customer da linha
                cutomer = review.split()[2]
                # Extraindo rating da linha
                rating = int(review.split()[4])
                # Extraindo votes da linha
                votes = int(review.split()[6])
                # Extraindo helpful da linha
                helpful = int(review.split()[8])
                # Adicionando as informações no dicionário para ser usado posteriormente
                product["reviews"].append({"customer": cutomer, "rating":rating, "votes":votes, "helpful":helpful, "date":data})
       
            
        
            
        

