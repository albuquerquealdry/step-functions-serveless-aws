import boto3

def create_files(file_names):
    for file_name in file_names:
        try:
            with open(file_name, 'w') as file:
                file.write("Este é o conteúdo do arquivo " + file_name)
            print(f"Arquivo '{file_name}' criado com sucesso.")
        except Exception as e:
            print(f"Erro ao criar o arquivo '{file_name}': {e}")

def upload_files_to_s3(bucket_name, file_names):
    s3 = boto3.client('s3')

    for file_name in file_names:
        try:
            # Upload do arquivo para o S3
            s3.upload_file(file_name, bucket_name, file_name)
            print(f"Arquivo '{file_name}' carregado com sucesso para o bucket '{bucket_name}'.")
        except Exception as e:
            print(f"Erro ao carregar o arquivo '{file_name}': {e}")

if __name__ == "__main__":
    bucket_name = 'recebimentodedados-aws-mock1'  # Substitua pelo nome do seu bucket S3

    # Lista de nomes dos arquivos a serem criados e enviados
    file_names = [str(num) + '.txt' for num in range(101)]

    # Criar arquivos localmente
    create_files(file_names)

    # Enviar arquivos para o S3
    upload_files_to_s3(bucket_name, file_names)
