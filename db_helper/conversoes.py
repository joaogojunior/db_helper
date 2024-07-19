# cria uma string com as colunas devidamente escapadas e separadas por virgula, podendo opcionalmente, estar toda a
# string entre parêntesis. A string é criada a partir de uma lista de colunas e uma lista de tabelas validas. Se for
# encontrado uma coluna ou tabela inválida, retorna `COLUNA INVALIDA` no seu lugar.
# ex: Entrada:  ['login', 'hash', 'salt'], ['usuario'], True  Saida: '(`login`, `hash`, `salt`)'
# ex: Entrada:  ['invalido1', 'invalido2'], ['usuario'], True  Saida: '(`COLUNA INVALIDA`, `COLUNA INVALIDA`)'
# ex: Entrada:  ['login', 'hash'], ['invalido'], True  Saida: '(`COLUNA INVALIDA`, `COLUNA INVALIDA`)'
# caso especial: Entrada [], [], True  Saida: '(*)'.
def concatena_colunas_separados_por_virgula_str(objeto, colunas, tabelas=(), parentesis=True):
    if objeto.debug:
        print('concatena_colunas_em_str_separados_por_virgula iniciado com:', colunas, tabelas, parentesis)
    # tabelas é utilizado apenas para testar as colunas
    # colunas ja foram convertidas em listas
    # aplica filtro checa_coluna ja adicionando aspas
    colunas = list(map(lambda col: objeto.checa_coluna(col, tabelas, objeto.get_db_col_esc()), colunas))
    # composição condicional de string
    return_val = ((parentesis * "(") + ", ".join(colunas) * (len(colunas) > 0) +
                  "*" * (len(colunas) == 0) + (")" * parentesis))
    if objeto.debug:
        print("concatena_colunas_em_str_separados_por_virgula retorno:", return_val)
    return return_val


# cria uma string que consiste de igualdades com os valores delimitados por aspas apropriadas e separados por
# vírgula.
def concatena_listas_em_pares_chave_valor_str(objeto, colunas, valores, tabelas, join_str=", "):
    if objeto.debug:
        print('concatena_listas_em_pares_chave_valor - iniciado com:', tabelas, colunas, valores, join_str)
    # tabelas ja foram checadas
    # colunas e valores ja foram convertidos em lista
    # aplica filtro checa_coluna ja adicionando aspas
    colunas = converte_em_lista(colunas, lambda col: objeto.valida_e_escapa_coluna(col, tabelas))
    # aplica filtro converte_item_em_str
    valores = converte_em_lista(valores, lambda item: objeto.converte_atributo_str(item, tabelas))
    return_val = (join_str.join(list(map(lambda par: "%s = %s" % (par[0], par[1]), zip(colunas, valores)))))
    if objeto.debug:
        print('concatena_listas_em_pares_chave_valor - retornando:', return_val)
    return return_val


# converte a entrada em item em uma string que será escapada (contida) em aspas apropriadas de acordo com o fato de
# ser uma coluna válida uma string de atributo.
# Para isso este codigo re-utiliza as checagens em checa_coluna.
# Ex: Entrada: "login", "usuario"  Saida: '`login`'
# Ex: Entrada: "atributo", 'usuario'  Saida: "'atributo'"
def converte_atributo_str(objeto, item, tabelas):
    match item:
        case int() | float():
            return str(item)
        case str():
            col = objeto.valida_e_escapa_coluna(item, tabelas)
            match col:
                case "`COLUNA INVALIDA`":
                    # eh um atributo normal
                    # escapa item com aspas de atributo
                    return objeto.get_db_attr_esc() + item + objeto.get_db_attr_esc()
                case _:
                    # eh uma coluna
                    # retorna coluna ja escapada
                    return col
        case _:
            raise Exception("erro! objeto fora do especificado: %s tipo: %s" % (str(item), type(item)))


# Converte diversos tipos em uma lista, ou encapsulando o item em uma lista ou no caso de tupla duplicando
# o conteudo original, difere do metodo padrao de conversao str em list que faz uma string ser tratada como
# uma lista de cada caractere em oposição ao encapsular a string por inteiro como um item na lista.
# Opcionalmente roda um map na lista criada utilizando uma funcao externa especificada...
def converte_em_lista(var_lst, func=None):
    # faz input uma lista mesmo se for None ou str e se for uma tupla insere em uma lista
    match var_lst:
        case None:
            var_lst = list()
        case str() | int() | float() | tuple():
            var_lst = [var_lst]
        case list():
            pass
        case _:
            raise Exception("converte_em_lista - erro! objeto fora do especificado: %s tipo: %s" % (str(var_lst),
                                                                                                    type(var_lst)))
    # se fornecido checa e filtra com um metodo customizado
    if func is None:
        return var_lst
    else:
        return list(map(func, var_lst))
