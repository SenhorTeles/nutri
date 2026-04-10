from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def automacao_sieg():
    # Inicializando o Chrome com opções
    options = webdriver.ChromeOptions()
    options.add_argument('--start-maximized')
    
    print("Iniciando o navegador...")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20) # Aumentando o tempo de espera para garantir que os elementos carreguem
    
    try:
        # 1. Acessando o site
        print("Acessando o Portal Hub Sieg...")
        driver.get("https://hub.sieg.com/")
        
        # 1.5 Clicar em Aceitar Cookies se aparecer
        try:
            print("Verificando banner de cookies...")
            # Esperamos até 5 segundos caso ele demore um pouco para pipocar na tela
            btn_cookies = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'cc-ALLOW') or text()='Aceitar Cookies']")))
            driver.execute_script("arguments[0].click();", btn_cookies)
            print("Cookies aceitos com sucesso.")
            time.sleep(1) # Pausa rápida para a animação do banner sumir
        except:
            print("Nenhum banner de cookies foi encontrado ou já foi aceito.")
        
        # 2. Preenchendo Login e Senha
        print("Preenchendo credenciais...")
        email_input = wait.until(EC.presence_of_element_located((By.ID, "txtEmail")))
        email_input.clear()
        email_input.send_keys("sollar1@somacontabilidades.com.br")
        
        senha_input = driver.find_element(By.ID, "txtPassword")
        senha_input.clear()
        senha_input.send_keys("102030@Af")
        
        # 3. Clicando em Entrar
        print("Efetuando login...")
        btn_entrar = driver.find_element(By.ID, "btnSubmit")
        btn_entrar.click()
        
        # 4. Esperando a tela carregar e clicando na empresa "DNL COMERCIO E SERVICOS LTDA"
        print("Aguardando carregamento da lista de clientes...")
        # Procuramos o span que contém o CNPJ ou o Nome e pegamos a linha (tr) parente
        xpath_cliente = "//span[contains(text(), '36899766000105')]/ancestor::tr"
        
        # Primeiro, esperamos que o elemento exista na tela
        wait.until(EC.presence_of_element_located((By.XPATH, xpath_cliente)))
        time.sleep(2) # Pausa estratégica para dar tempo do overlay de carregamento da plataforma (se houver) sumir
        
        cliente_row = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_cliente)))
        
        # Scroll até o elemento caso não esteja visível
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", cliente_row)
        time.sleep(1) # Breve pausa após o scroll
        
        try:
            cliente_row.click()
        except:
            # Se o clique normal falhar (por exemplo, bloqueado por um header fixo), forçamos via JS
            driver.execute_script("arguments[0].click();", cliente_row)
        
        # Como o evento onclick tem window.open com '_blank', o navegador abre uma nova aba.
        # Precisamos mudar o foco do driver para a nova aba.
        print("Mudando para a nova aba de detalhes do cliente...")
        wait.until(lambda d: len(d.window_handles) > 1)
        driver.switch_to.window(driver.window_handles[-1])
        
        # 5. Clicando em Download CT-e
        print("Aguardando carregamento da página do cliente e clicando em Download CT-e...")
        # Procuramos o botão que tem class 'btn-download-dashboard' e 'cte' no envio
        xpath_btn_cte = "//a[contains(@class, 'btn-download-dashboard') and contains(@onclick, 'cte')]"
        
        wait.until(EC.presence_of_element_located((By.XPATH, xpath_btn_cte)))
        time.sleep(2)
        
        btn_download_cte = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_btn_cte)))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn_download_cte)
        time.sleep(1)
        try:
            btn_download_cte.click()
        except:
            driver.execute_script("arguments[0].click();", btn_download_cte)
        
        # 6. Clicando na aba "Chaves"
        print("Clicando na aba Chaves...")
        # Esperando modal ou aba renderizar
        time.sleep(1)
        aba_chaves = wait.until(EC.element_to_be_clickable((By.ID, "keys-tab")))
        try:
            aba_chaves.click()
        except:
            driver.execute_script("arguments[0].click();", aba_chaves)
        
        print("Automação finalizada com sucesso! O navegador permanecerá aberto.")
        # Mantém o script rodando um pouco para você ver o resultado
        time.sleep(30)
        
    except Exception as e:
        print(f"Ocorreu um erro durante a automação: {e}")
        # Tira um print da tela no momento do erro para ajudar na depuração
        driver.save_screenshot('erro_sieg.png')
        print("Screenshot salva como 'erro_sieg.png'")
        
    finally:
        # Se quiser que o navegador feche automaticamente, descomente a linha abaixo
        # driver.quit()
        print("Finalizando execução do script.")

if __name__ == "__main__":
    automacao_sieg()
