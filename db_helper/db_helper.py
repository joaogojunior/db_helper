# Implementação de uma classe singleton utilizando pymysql capaz de conectar-se com mysql / mariadb e
# fazer operações básicas como: conectar, fazer querys, inserts, updates e deletes de maneira simplificada.
#
#

import pymysql.cursors
from threading import Lock
from db_helper.conversoes import (converte_em_lista, concatena_colunas_separados_por_virgula_str,
                                  concatena_listas_em_pares_chave_valor_str)
from db_helper.validacoes_tabelas import escapa_coluna, valida_tabela, valida_e_escapa_coluna
from criador_json import criador_json as cj

VERSAO = "0.1"


# Implementação de singleton usando metaclass, fonte:
# https://stackoverflow.com/questions/51896862/how-to-create-singleton-class-with-arguments-in-python


class Singleton(type):
    __instance = None

    def __call__(cls, *args, **kwargs):
        if cls.__instance is None:
            # se valor de debug for verdadeiro nos parametros de inicializacao da classe, mostra mensagens
            # de debug aqui tambem.
            if "debug" in kwargs and kwargs["debug"]:
                print("Iniciando a instância singleton...")
            cls.__instance = super(Singleton, cls).__call__(*args, **kwargs)
        # mostra mensagens de debug se for o caso.
        if "debug" in kwargs and kwargs["debug"]:
            print("Retornando a instância do singleton!")
        return cls.__instance


class DbHelper(metaclass=Singleton):
    # strings e caracteres especiais do mysql
    __db_col_esc = "`"
    __db_attr_esc = "'"
    __db_verbs = {
        'insert': "INSERT INTO %s %s VALUES ",
        # 'v;': " (%s);",
        'delete': "DELETE FROM %s WHERE %s",
        '=v;': " = %s;",
        'update': "UPDATE %s SET %s WHERE %s",
        'select': "SELECT %s FROM %s",
        'join': " JOIN ",
        'where': " WHERE ",
        'and': " AND ",
        'orderby': " ORDER BY %s ",
        'asc': "ASC",
        'desc': "DESC",
        'or': " OR ",
        'like': " LIKE %s"
    }

    # dados da conexao
    _db_connection = None
    _db_host = ""
    _db_username = ""
    _db_password = ""
    _db_schema = ""

    # inicializa cache
    _colunas_validas_cache = dict()
    _tabelas_validas_cache = None

    # contador de querys
    _db_contador = 0

    # sinaliza se mostra mensagens de debug (vai ser inicializado pelo __init__)
    debug = None

    # valida entrada, configura e inicia a conexão com banco de dados
    def __init__(self, hostname='', username='', password='', schema='',
                 config="db_config.json", debug=False):
        # atualiza valor de debug
        self.debug = debug
        if self.debug:
            print("Iniciando db_helper %s" % VERSAO)

        if hostname == '' and username == '' and password == '' and schema == '':
            # configura e inicia conexao a partir do j-son
            self.configura_conexao_json(config)
        elif hostname != '' and username != '' and password != '' and schema != '':
            # configura e inicia conexao a partir dos dados passados na criação do objeto
            self.set_db_host(hostname)
            self.set_db_username(username)
            self.set_db_password(password)
            self.set_db_schema(schema)
            self.db_connect()

    # getter para __db_col_esc
    def get_db_col_esc(self):
        return self.__db_col_esc

    # getter para dicionario __db_verbs
    def get_db_verbs(self, chave=""):
        if chave == "":
            # retorna dicionario inteiro
            return self.__db_verbs
        else:
            # retorna conteudo da chave, se nao existir retorna None
            return self.__db_verbs.get(chave, None)

    # getter para __db_attr_esc
    def get_db_attr_esc(self):
        return self.__db_attr_esc

    # getter para contador de querys, garante que o retorno sera um valor da contagem, sem repetições.
    def le_e_incrementa_contador(self):
        # faz essa operaçao atomica
        lock = Lock()
        with lock:
            if self.debug:
                print("Entrando no setor critico do contador...")
            # setor critico
            # tira uma copia do valor atual
            contador_atual = self._db_contador
            # e incrementa contador
            if self.debug:
                print("Incremetando o contador...")
            self._db_contador += 1
        if self.debug:
            print("Saindo do setor critico do contador...")
        # fora da setor critico
        # mesmo que essa funcao seja executada duas em paralelo ao chegar no lock apenas uma prosseguira
        # assim contador_atual é garantido de ter sempre um valor unico para cada execução de get_inc_cont
        # pois o atributo compartilhado self._db_contador tem sua leitura limitada a uma thread por vez e
        # so tem a leitura liberada apos o incremento da mesma
        return contador_atual

    # setter para _db_schema
    def set_db_schema(self, schema):
        self._db_schema = schema

    # setter para _db_host
    def set_db_host(self, host):
        self._db_host = host

    # setter para _db_username
    def set_db_username(self, username):
        self._db_username = username

    # setter para _db_password
    def set_db_password(self, password):
        self._db_password = password

    # conecta ao bando de dados. Retorna True se a conexão ocorrer sem erros ou False para todos os outros casos.
    def db_connect(self):
        try:
            self._db_connection = pymysql.connect(host=self._db_host,
                                                  user=self._db_username,
                                                  password=self._db_password,
                                                  db=self._db_schema,
                                                  charset='utf8mb4',
                                                  cursorclass=pymysql.cursors.DictCursor)
            if self.debug:
                print("Conectado com sucesso!")
            return True
        except Exception as e:
            # imprime o erro na tela
            if self.debug:
                print("Erro conectando ao db!", str(e))
            return False

    # configura parametros da conexão com base em um arquivo j-son e inicia a conexao com o banco de dados. Retorna
    # True se a conexão ocorrer sem problemas ou False em todos os outros casos.
    def configura_conexao_json(self, arq):
        if self.debug:
            print("Checando se já existe uma conexão...")
        if self._db_connection is None:
            if self.debug:
                print("Abrindo configuracao do db..")
            dados_padrao = {
                "hostname": "localhost",
                "username": "",
                "password": "",
                "schema": "deliveryadm",
                "json_ver": 1
            }
            config = cj.carrega_ou_cria_config(arq, dados_padrao)
            try:
                self.set_db_host(config["hostname"])
                self.set_db_username(config["username"])
                self.set_db_password(config["password"])
                self.set_db_schema(config["schema"])
            except KeyError:
                print("Não foi possível configurar a conexão, abortando...")
                return False
            if self.debug:
                print("Conectando ao db...")
            return self.db_connect()
        if self.debug:
            print("Conexão já iniciada previamente, saindo...")
        return True

    def _db_fetch_all(self, sql, argumentos=None, contador=0):
        if self._db_connection is None:
            return -1, \
                "c = %d - Erro! Não é possível executar comandos antes de conectar... use dbConnect() apos configurar \
                host, usuario e password." % contador, 0
        else:
            try:
                with self._db_connection.cursor() as cursor:
                    if self.debug:
                        print("c = %d - _db_fetch_all - ping connection..." % contador)
                    self._db_connection.ping(reconnect=True)
                    if self.debug:
                        print("c = %d - debug sql -" % contador, cursor.mogrify(sql, argumentos))
                    quantidade_rows_afetadas = cursor.execute(sql, argumentos)
                    if self.debug:
                        print('c = %d - _db_fetch_all - qtd rows: %s' % (contador, quantidade_rows_afetadas))
                    result = cursor.fetchall()
                    # self._db_connection.close()
                return result, "Ok!", quantidade_rows_afetadas
            except Exception as e:
                # exit(1)
                return -1, "c = %d - Erro! " % contador + str(e), 0

    def _db_commit(self, sql, argumentos=None, contador=0):
        if self._db_connection is None:
            return -1, \
                "c = %d - Erro! Não é possível executar comandos antes de conectar... use dbConnect() apos configurar \
                host, usuario e password." % contador, 0
        else:
            try:
                with self._db_connection.cursor() as cursor:
                    if self.debug:
                        print('c = %d - _db_commit - ping connection...' % contador)
                    self._db_connection.ping(reconnect=True)
                    if self.debug:
                        print("c = %d - debug sql -" % contador, cursor.mogrify(sql, argumentos))
                    quantidade_rows_afetadas = cursor.execute(sql, argumentos)
                    if self.debug:
                        print('c = %d - _db_commit - qtd rows: %s | row id: %s' % (
                            contador, quantidade_rows_afetadas, cursor.lastrowid))
                    self._db_connection.commit()
                    # self._db_connection.close()
                return cursor.lastrowid, "Ok!", quantidade_rows_afetadas
            except Exception as e:
                return -1, "c = %d - Erro! " % contador + str(e), 0

    def db_insert(self, tabela, colunas, valores):
        # garante que tabela eh valida
        tabela = valida_tabela(self, tabela)
        # garante que entradas sao listas
        # se alguma lista for None converte em [] e se for lista mesmo permanece igual
        colunas = converte_em_lista(colunas)
        # valores vai ser filtrado internamente pelo pymysql no commit
        valores = converte_em_lista(valores)
        sql = (DbHelper.__db_verbs['insert'] % (
            escapa_coluna(self, tabela), concatena_colunas_separados_por_virgula_str(colunas, [tabela], True))
               + '(' + ', '.join(['%s'] * len(valores)) + ');')
        if self.debug:
            print('insert debug -', sql)
            print('insert debug valores:', valores)
        contador = self.le_e_incrementa_contador()
        return self._db_commit(
            sql,
            valores,
            contador
        )

    def db_delete(self, tabela, varteste, valor):
        # garante que tabela eh valida
        tabela = valida_tabela(self, tabela)
        # varteste vai ser testado dentro de checa_coluna
        # valor vai ser filtrado internamente pelo mysql no commit
        sql = (self.get_db_verbs('delete') % (
            escapa_coluna(self, tabela), valida_e_escapa_coluna(self, varteste, [tabela]))
               + self.get_db_verbs('=v;'))
        contador = self.le_e_incrementa_contador()
        return self._db_commit(sql, valor, contador)

    def db_update(self, tabela, colunas, valores, varteste, valor):
        # garante que tabela eh valida
        tabela = valida_tabela(self, tabela)
        # garante que entradas sao listas
        # se alguma lista for None converte em [] e se for lista mesmo permanece igual
        colunas = converte_em_lista(colunas)
        valores = converte_em_lista(valores)
        # varteste vai ser testado dentro de checa_coluna
        # valor vai ser filtrado internamente pelo mysql no commit
        sql = (self.get_db_verbs('update') % (
            escapa_coluna(self, tabela), concatena_listas_em_pares_chave_valor_str(self, colunas, valores, [tabela]),
            valida_e_escapa_coluna(self, varteste, [tabela])) + self.get_db_verbs('=v;'))
        contador = self.le_e_incrementa_contador()
        return self._db_commit(sql, valor, contador)

    def db_query_col(self, tabelas, colunas, var_teste_list=None, gui_valor_list=None, orderby=None, ascendent=True,
                     colunas_test_list=None, valores_test_list=None):
        # tabelas - lista de tabelas que serao usadas na query se mais de uma for especificada usa JOIN para uni-las.
        # colunas - lista de colunas que serao retornadas na query se uma coluna for especificada em uma tupla junto
        #           com um alias sera usada a clausula AS para especificar o alias, se for especificado uma lista
        #           vazia sera utilizado `*` na query.
        # var_teste_list - lista de atributos (colunas) a serem comparadas com literals que serao fornecidos pelo
        #           usuario em gui_valor_list ambos utilizados na clausula WHERE
        # gui_valor_list - lista de literals fornecidos pelo usuarios
        # orderby - titulo da coluna que sera usada pra ordernar os dados de forma ascendente ou decrescente
        #           utilizando o parametro boleano ascendent.
        # ascendent - True ou False indica a ordem em que os resultados serão apresentados é utilizando em conjunto
        #           com orderby.
        # colunas_test_list - lista de colunas a serem comparadas na clausula where
        # valores_test_list - lista de valores a serem comparadas ma clausula WHERE

        # garante que todos os parametros que aceitam listas sao listas
        # adicionalmente filtra com checa_tabela
        tabelas = converte_em_lista(tabelas, valida_tabela)
        # colunas sera filtrado por checa_colunas
        colunas = converte_em_lista(colunas)
        # var_teste_list sera filtrado por checa_coluna
        var_teste_list = converte_em_lista(var_teste_list)
        # faz gui_valor_list uma lista, seu conteudo sera checado internamente pelo mysql commit
        gui_valor_list = converte_em_lista(gui_valor_list)
        # colunas_test_list sera filtrado por checa_coluna
        colunas_test_list = converte_em_lista(colunas_test_list)
        # valores_test_list sera filtrado por converte_item_str
        valores_test_list = converte_em_lista(valores_test_list)

        # monta string sql sem uso de condicional (rende todas as substrings e descartas as nao aplicaveis)
        sql = (self.get_db_verbs('select') % (
            concatena_colunas_separados_por_virgula_str(colunas, tabelas, False),
            self.get_db_verbs('join').join(list(map(lambda tab: escapa_coluna(self, tab), tabelas)))
        ) + self.get_db_verbs('where') * (len(var_teste_list) > 0 or len(colunas_test_list) > 0) +
               (self.get_db_verbs('and').join(converte_em_lista(var_teste_list, lambda col: valida_e_escapa_coluna(
                   self, col, tabelas) + " = %s"))) * (len(var_teste_list) > 0) +
               self.get_db_verbs('and') * (len(colunas_test_list) > 0 and len(var_teste_list) > 0) +
               concatena_listas_em_pares_chave_valor_str(self, colunas_test_list, valores_test_list, tabelas,
                                                         self.get_db_verbs('and')) + self.get_db_verbs('orderby')
               % valida_e_escapa_coluna(self, orderby, tabelas) * (orderby is not None) +
               self.get_db_verbs('asc') * (orderby is not None and ascendent) +
               self.get_db_verbs('desc') * (orderby is not None and not ascendent) + ";")

        # gui_valor_list sera sanitizada pelo pymsql
        tupla = (sql, None) * (len(gui_valor_list) == 0) + (sql, gui_valor_list) * (len(gui_valor_list) > 0)

        contador = self.le_e_incrementa_contador()
        # db_fetch_all(*tupla) - argument packing
        ret = self._db_fetch_all(*tupla, contador)
        if self.debug:
            print("c = %d - db_query_col ret -" % contador, ret)
        return ret

    def db_query_col_like(
            self, tabela, colunas, lista_vartestes=None, lista_valores=None, orderby=None, ascendent=True):
        # tabela - tabela a ser usada na query
        # colunas - lista de colunas ou atributos que serao retornado na query
        # lista_varteste - lista de colunas a serem testadas com os parametros literals fornecidos pelo usuario
        #                  e utilizado na clausula where
        # valores - lista de valores fornecidos pelo usuario proveniente da gui
        # orderby - atributo opcional a ser utilizado para ordernar os resultados da query
        # ascendent - True ou False, parametro que define se a ordem da ordenação é ascendente ou decrescente

        # filtra tabela com checa_tabela
        tabela = valida_tabela(self, tabela)
        # faz listas todas os parametros de entrada que aceitam listas
        colunas = converte_em_lista(colunas)
        lista_vartestes = converte_em_lista(lista_vartestes)
        # lista_valores é uma sub string ou lista de sub strings fornecidos pelo usuario para procura
        termos_procura = converte_em_lista(lista_valores, lambda valor: "%" + str(valor) + "%")

        # composição condicional de string
        sql = (self.get_db_verbs('select') % (
            concatena_colunas_separados_por_virgula_str(
                colunas, [tabela], False), escapa_coluna(self, tabela)
        ) + self.get_db_verbs('where') + self.get_db_verbs('or').join(
            list(map(lambda varteste: varteste + self.get_db_verbs('like'),
                     converte_em_lista(lista_vartestes, lambda col: valida_e_escapa_coluna(self, col, [tabela]))))
        ) + self.get_db_verbs('orderby') % valida_e_escapa_coluna(self, orderby, [tabela]) * (orderby is not None)
               + self.get_db_verbs('asc') * (orderby is not None and ascendent is True) +
               self.get_db_verbs('desc') * (orderby is not None and ascendent is False) + ";")

        tupla = (sql, None) * (len(termos_procura) == 0) + (sql, termos_procura) * (len(termos_procura) > 0)
        contador = self.le_e_incrementa_contador()
        ret = self._db_fetch_all(*tupla, contador)
        if self.debug:
            print("c = %d - db_query_col_like ret -" % contador, ret)
        return ret

    def db_le_titulo_colunas_da_tabela_com_cache(self, tabela):
        if self.debug:
            print("Iniciando db_get_cached_column_names_from_table...")
        # checa se a tabela é valida
        tabela = valida_tabela(self, tabela)
        if tabela == "TABELA INVALIDA":
            if self.debug:
                print("Tabela invalida retornando lista vazia...")
            return list()
        # guarda um cache das colunas validas pra as tabelas
        if tabela not in self._colunas_validas_cache.keys():
            if self.debug:
                print("Tabela %s não foi encontrada no cache, obtendo do banco de dados..." % tabela)
            c = self.le_e_incrementa_contador()
            res, msg, qtd = self._db_fetch_all(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s;", tabela, c)
            if res == -1:
                if self.debug:
                    print("Erro! Banco de dados retornou -1, não conectado?")
                raise Exception(msg)
            self._colunas_validas_cache[tabela] = (list(map(lambda d: d['COLUMN_NAME'], res)) +
                                                   list(map(lambda d: tabela + "." + d['COLUMN_NAME'], res)))
            if self.debug:
                print("db_get_column_names_from_table - atualizando cache com: %s" % tabela,
                      self._colunas_validas_cache[tabela])
        else:
            if self.debug:
                print("db_get_column_names_from_table - retornando valor no cache: %s" % tabela,
                      self._colunas_validas_cache[tabela])
        return self._colunas_validas_cache[tabela]
