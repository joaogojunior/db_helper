from db_helper.conversoes import converte_em_lista


def lista_colunas_validas(self, tabelas):
    # garante que tabelas é uma lista
    tabelas = converte_em_lista(tabelas)
    # garante que o retorno é uma lista sempre
    return sum(list(map(self.db_le_titulo_colunas_da_tabela_com_cache, tabelas)), [])


def lista_tabelas_validas(self):
    if self._tabelas_validas_cache is None:
        c = self.le_e_incrementa_contador()
        res, msg, qtd = self._db_fetch_all(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", self._db_schema, c)
        if res == -1:
            raise Exception(msg)
        self._tabelas_validas_cache = list(map(lambda d: d['table_name'], res))
        if self.debug:
            print("lista_tabelas_validas - atualizando cache com: %s" % self._tabelas_validas_cache)
    return self._tabelas_validas_cache


def valida_tabela(self, tabela):
    match tabela.split("."):
        case [tab] if tab in lista_tabelas_validas(self):
            return tabela
        case [self._db_schema, tab] if tab in lista_tabelas_validas(self):
            return tab
        case _:
            return "TABELA INVALIDA"


# utiliza o caractere de escape para colunas do mariadb
def escapa_coluna(self, coluna):
    aspas = self.get_db_col_esc()
    return aspas + coluna + aspas


def valida_e_escapa_coluna(self, item, tabelas):
    if self.debug:
        print("checa_coluna iniciado com:", item, tabelas)
    colunas_validas = lista_colunas_validas(self, tabelas)
    match item:
        # se for None
        case None:
            if self.debug:
                # retorna None quando variaveis nao usadas sao setadas como None
                print("checa_coluna - retorna None")
            return None
        # se for string ou lista com string
        case str(coluna) | [coluna]:
            # coluna pode ter palavras separadas por pontos
            match coluna.split("."):
                # suporta caso que so tem a coluna sem pontos
                case [col] if col in colunas_validas:
                    if self.debug:
                        print('checa_coluna - retorna', self.escapa_coluna(col))
                    return self.escapa_coluna(col)
                # suporta caso que veio uma string tipo tabela.coluna
                case [tab, col] if tab + "." + col in colunas_validas:
                    if self.debug:
                        print('checa_coluna - retorna', self.escapa_coluna(tab) + "." + self.escapa_coluna(col))
                    return self.escapa_coluna(tab) + "." + self.escapa_coluna(col)
                # suporta caso que veio uma string tipo deliveryadm.tabela.coluna
                case [self._db_schema, tab, col] if tab + "." + col in colunas_validas:
                    if self.debug:
                        print('checa_coluna - retorna', self.escapa_coluna(tab) + "." + self.escapa_coluna(col))
                    return self.escapa_coluna(tab) + "." + self.escapa_coluna(col)
            # coluna invalida
            if self.debug:
                print('checa_coluna - retorna ' + self.escapa_coluna("COLUNA INVALIDA"))
            return self.escapa_coluna("COLUNA INVALIDA")
        # se for lista ou uma tupla que possua uma coluna seguida por um alias
        case [str(coluna), str(alias)] | (str(coluna), str(alias)):
            match coluna.split('.'):
                case [col] if col in colunas_validas:
                    if self.debug:
                        print("checa_coluna - retorna", self.escapa_coluna(col) + " AS " + alias)
                    return self.escapa_coluna(col) + " AS " + alias
                case [tab, col] if tab + "." + col in colunas_validas:
                    if self.debug:
                        print("checa_coluna - retorna", self.escapa_coluna(tab) + "." + self.escapa_coluna(col) +
                              " AS " + alias)
                    return self.escapa_coluna(tab) + "." + self.escapa_coluna(col) + " AS " + alias
                case [self._db_schema, tab, col] if tab + "." + col in colunas_validas:
                    if self.debug:
                        print("checa_coluna - retorna",
                              self.escapa_coluna(tab) + "." + self.escapa_coluna(col) + " AS " + alias)
                    return self.escapa_coluna(tab) + "." + self.escapa_coluna(col) + " AS " + alias
            # coluna invalida
            if self.debug:
                print("checa_coluna - retorna", self.escapa_coluna("COLUNA INVALIDA") + " AS " + alias)
            return self.escapa_coluna("COLUNA INVALIDA") + " AS " + alias
        case _:
            if self.debug:
                print("checa_coluna caiu no caso catch-all! retornando " + self.escapa_coluna("COLUNA INVALIDA"))
            return self.escapa_coluna("COLUNA INVALIDA")
