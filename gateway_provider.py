import mysql.connector
from mysql.connector import Error

# Função para conectar ao Banco de Dados
def split_db():
    try:
        # Estabelecendo a conexão
        connection = mysql.connector.connect(
            host="localhost",      # Servidor local
            database="bd_test",    # Nome do banco de dados
            user="root",           # Nome de usuário
            password=""            # Senha em branco
        )
        if connection.is_connected():
            print("Conectado ao banco de dados com sucesso!")
            return connection
    except Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

#Gerar dados para ajuste 
def query_provider(connection):
    try:
        query="""
        SELECT
            tqp.id,
            tqp.gateway_provider,
            tqp.id_proposal ,
            tp.status
        FROM TB_QUOTATION_PROPOSAL tqp 
        INNER JOIN TB_POLICIES tp on tp.id_proposal = tqp.id_proposal 
        WHERE tqp.gateway_provider ='hinova'
        AND tp.status ='active';
        """
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        print("Erro ao buscar dados", e)
        return []

def update_provider(connection, gateway_provider, tqp_id):
    try:
        query = """
        UPDATE TB_QUOTATION_PROPOSAL
        SET gateway_provider = %s
        WHERE id = %s;
        """
        cursor = connection.cursor()
        cursor.execute(query, (gateway_provider, tqp_id))
        connection.commit()
        print(f"Registro com ID {tqp_id} atualizado para gateway_provider = '{gateway_provider}'!")
    except Error as e:
        print(f"Erro ao atualizar o registro com ID {tqp_id}:", e)
    finally:
        if cursor:
            cursor.close()

def main(connection):
    # Buscar registros que atendem aos critérios
    registros = query_provider(connection)

    # Atualizar cada registro encontrado
    for registro in registros:
        tqp_id = registro['id']  # ID do registro
        novo_gateway_provider = "asaas"  # Novo valor para o campo gateway_provider
        update_provider(connection, novo_gateway_provider, tqp_id)
    
# Fechar conexão de maneira segura
def close_connection(connection):
    if connection is not None and connection.is_connected():
        connection.close()
        print("Conexão ao banco de dados foi encerrada.")

connection = split_db()