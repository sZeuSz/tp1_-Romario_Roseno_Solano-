import psycopg2
from prettytable import PrettyTable
from dotenv import load_dotenv
import os
load_dotenv()

# conexão com o banco de dados
conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

cur = conn.cursor()
while True:
    print("Selecione uma opção:")
    print("1. Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
    print("2. Dado um produto, listar os produtos similares com maiores vendas do que ele")
    print("3. Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada")
    print("4. Listar os 10 produtos líderes de venda em cada grupo de produtos")
    print("5. Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
    print("6. Listar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
    print("7. Listar os 10 clientes que mais fizeram comentários por grupo de produto")
    print("0. Sair")

    # aguarde a entrada do usuário
    opcao = input("Opção: ")

    # execute a opção selecionada
    if opcao == "1":
        product_asin = input("Digite o ASIN do produto: ")
        sql = """
            SELECT *
            FROM (
                SELECT to_char(reviews.date, 'YYYY-MM-DD') AS date,
                    reviews.customer_cid AS customer_cid,
                    reviews.rating AS rating,
                    reviews.votes AS votes,
                    reviews.helpful AS helpful
                FROM reviews JOIN products ON reviews.product_id = products.id
                WHERE products.asin = %s
                ORDER BY rating DESC, helpful DESC
                LIMIT 5
            ) AS top_pos
            UNION ALL
            SELECT *
            FROM (
                SELECT to_char(reviews.date, 'YYYY-MM-DD') AS date,
                    reviews.customer_cid AS customer_cid,
                    reviews.rating AS rating,
                    reviews.votes AS votes,
                    reviews.helpful AS helpful
                FROM reviews JOIN products ON reviews.product_id = products.id
                WHERE products.asin = %s
                ORDER BY rating ASC, helpful DESC
                LIMIT 5
            ) AS top_neg;
            """
        # executar a consulta

        cur.execute(sql, (product_asin, product_asin))
        rows = cur.fetchall()

        # criar a tabela com as colunas desejadas
        table = PrettyTable(['date', 'customer_cid', 'rating', 'votes', 'helpful'])

        # adicionar as linhas na tabela
        for row in rows:
            table.add_row([row[0], row[1], row[2], row[3], row[4]])

        # imprimir a tabela
        print(table)
    elif opcao == '2':
        product_asin = input("Digite o ASIN do produto: ")
        sql = """
          SELECT products.asin, products.title, products.salesrank, products.group_name
          FROM product_similars
          JOIN products ON product_similars.similar_product_asin = products.asin
          WHERE product_similars.product_asin = %s
          AND products.salesrank > (SELECT salesrank FROM products WHERE asin = %s)
          ORDER BY products.salesrank ASC;
        """

        # executar a consulta
        cur.execute(sql, (product_asin, product_asin))
        rows = cur.fetchall()
        # criar a tabela com as colunas desejadas
        table = PrettyTable(['product_asin', 'product_title', 'product_salesrank', 'product_group'])
        # adicionar as linhas na tabela
        for row in rows:
            table.add_row([row[0], row[1], row[2], row[3]])
        
        # imprimir tabela
        print(table)
    elif opcao == '3':
        product_asin = input("Digite o ASIN do produto: ")
        sql = """
            SELECT
                date_trunc('day', reviews.date) AS day, AVG(reviews.rating) AS average_rating
            FROM reviews
            JOIN products ON reviews.product_id = products.id
            WHERE products.asin = %s
            GROUP BY day
            ORDER BY day;
        """
        # executar a consulta
        cur.execute(sql, (product_asin,))
        rows = cur.fetchall()
        # criar a tabela com as colunas desejadas
        table = PrettyTable(['day', 'average_rating'])
        # adicionar as linhas na tabela
        for row in rows:
            table.add_row([row[0], row[1]])
        # imprimir tabela
        print(table)
    elif opcao == '4':
        # Fonte: https://www.sqlshack.com/sql-partition-by-clause-overview/
        # Desconsiderei os salesrank com -1 porque falaram no discord.
        sql = """
            SELECT p.asin, p.title, p.salesrank, p.group_name
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY salesrank ASC) as row_num
                FROM products
                WHERE salesrank != -1
            ) p
            WHERE p.row_num <= 10;
        """
        # executar a consulta
        cur.execute(sql)
        rows = cur.fetchall()
        # criar a tabela com as colunas desejadas
        table = PrettyTable(['product_asin', 'product_title', 'product_salesrank', 'product_group'])
        # adicionar as linhas na tabela
        for row in rows:
            table.add_row([row[0], row[1], row[2], row[3],])
        # imprimir tabela
        print(table)
    elif opcao == '5':
        # Desconsiderei os salesrank com -1 porque falaram no discord.
        sql = """
            SELECT p.asin, p.title, AVG(r.helpful) as avg_helpful
            FROM products p
            INNER JOIN reviews r ON p.id = r.product_id
            WHERE r.helpful > 0 AND p.salesrank > 0
            GROUP BY p.id
            ORDER BY avg_helpful DESC
            LIMIT 10;
        """

        # executar a consulta
        cur.execute(sql)
        rows = cur.fetchall()
        print(rows)
        # criar a tabela com as colunas desejadas
        table = PrettyTable(['product_asin', 'product_title', 'avg_helpful'])
        # adicionar as linhas na tabela
        for row in rows:
            table.add_row([row[0], row[1], row[2]])
        # imprimir tabela
        print(table)
    elif opcao == '6':
        sql = """
            SELECT pc.category_id, c.name, pc.product_id, AVG(r.helpful) as avg_helpful
            FROM product_categories pc
            JOIN categories c ON c.id = pc.category_id
            JOIN reviews r ON r.product_id = pc.product_id
            GROUP BY pc.category_id, c.name, pc.product_id
            ORDER BY AVG(r.helpful) DESC
            LIMIT 5;
        """
        # executar a consulta
        cur.execute(sql)
        rows = cur.fetchall()
        # criar a tabela com as colunas desejadas
        table = PrettyTable(['category_id', 'category_name','product_id', 'avg_helpful'])
        # adicionar as linhas na tabela
        for row in rows:
            table.add_row([row[0], row[1], row[2], row[3]])
        # imprimir tabela
        print(table)
    elif opcao == '7':
        sql = """
            SELECT p.group_name, c.cid, COUNT(*) AS num_reviews
            FROM products p
            JOIN reviews r ON r.product_id = p.id
            JOIN customers c ON c.cid = r.customer_cid
            GROUP BY p.group_name, c.cid
            ORDER BY p.group_name, num_reviews DESC
            LIMIT 10;
        """
        # executar a consulta
        cur.execute(sql)
        rows = cur.fetchall()
        # criar a tabela com as colunas desejadas
        table = PrettyTable(['group', 'customer_id','amount_comment'])
        # adicionar as linhas na tabela
        for row in rows:
            table.add_row([row[0], row[1], row[2]])
        # imprimir tabela
        print(table)
    else:
        print("Opção inválida. Tente novamente.")

# fechar a conexão com o banco de dados
cur.close()
conn.close()
