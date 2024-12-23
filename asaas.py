import requests
import mysql.connector  
from mysql.connector import Error

#conexão com o banco de dados
def split_db():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            database="sua_base_de_dados",  
            user="seu_usuario",       
            password="sua_senha"      
        )
        if connection.is_connected():
            print("Conectado com sucesso")
            return connection
    except Error as e:
        print("Erro ao conectar ao banco de dados:", e)
        return None

# Consulta os dados no banco e retona as informações para a criação do usuario 
def query_client(connection):
    try:
        query = """
        SELECT
            tu.name as name,
            tu.cpf as cpf_cnpj,
            tu.email as email,
            tu.phone as mobile_phone,
            ta.street as address,
            ta.`number` as address_number,
            ta.complement as complement,
            ta.district as province,
            ta.zip_code as postalCode,
            tu.id as external_reference,
            tw.id_asaas
        FROM TB_PROPOSAL tp
        INNER JOIN TB_USERS tu ON tu.id = tp.id_client
        INNER JOIN TB_ADDRESS ta ON ta.id = tp.id_address
        INNER JOIN TB_WALLET tw ON tw.id = tu.id_wallet
        WHERE tw.id_asaas IS NULL
        AND tp.id_author_partner = '8';
        """
        cursor = connection.cursor(dictionary=True)  # Retorna os dados como dicionário
        cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        print("Erro ao buscar dados:", e)
        return []

# Criando o cliente via API
def client_asaas(data):
    url = "https://sandbox.asaas.com/api/v3/customers"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "access_token": " "  
    }
    payload = {
    "name": data["name"],
    "email": data["email"],
    "address": data["address"],
    "province": data["province"],
    "postalCode": data["postalCode"],
    "cpfCnpj": data["cpf_cnpj"],
    "mobilePhone": data["mobile_phone"],
    "notificationDisabled": True,
    "addressNumber": data["address_number"],
    "complement": data["complement"],
    "externalReference": data["external_reference"]

    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 201:
            return response.json()["id_asaas"]
        else:
            print("Erro na API:", response.status_code, response.text)
            return None
    except Exception as e:
        print("Erro ao enviar requisição:", e)
        return None

#Registra o id_asaas 
def update_customer_id(connection, customer_id, db_id):
    try:
        query = "UPDATE TB_WALLET SET id_asaas = CONCAT('cus_000', %s) WHERE id = %s;"
        cursor = connection.cursor()
        cursor.execute(query, (customer_id, db_id))
        connection.commit()
        print(f"ID atualizado com sucesso: {customer_id} para o registro {db_id}")
    except Error as e:
        print("Erro ao atualizar o banco:", e)


def main():
    # Conecte ao banco de dados
    connection = split_db()
    if connection is None:
        return

    # Busque os dados necessários
    data_list = query_client(connection)
    if not data_list:
        print("Nenhum dado encontrado para processar.")
        return

    # Itere sobre os dados e envie as requisições
    for data in data_list:
        print(f"Processando registro ID: {data['id']}...")
        customer_id = client_asaas(data)
        if customer_id:
            # Atualize o banco com o ID retornado pela API
            update_customer_id(connection, customer_id, data["id"])

    # Feche a conexão com o banco
    connection.close()
    print("Processo concluído!")

# Executa o script
if __name__ == "__main__":
    main()
