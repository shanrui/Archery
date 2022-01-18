import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML
from sqlparse import tokens as T
import logging
logger = logging.getLogger('default')

def extract(parsed, table_name=None):
    context_tables = extract_table(parsed)
    columns = extract_column(parsed)
    return TmpTableInfo([None, table_name], table_name, table_name, columns, context_tables)

def extract_column(parsed):
    columns = []
    for item in parsed.tokens:
        if isinstance(item, Identifier):
            col = handle_column_identifier(item)
            columns.append(col)

        elif isinstance(item, IdentifierList):
            for child in item.get_identifiers():
                col =  handle_column_identifier(child)
                columns.append(col)

        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            return columns

def handle_column_identifier(c):
    logger.info("======== handle_column_identifier ========")   
    logger.info(c)
    logger.info(c.tokens)

    dot_count = 0
    for item in c.tokens:
        if item.value.upper() == '.':
            dot_count += 1
    if dot_count == 1:
        return ColumnInfo([None, c.get_parent_name(), c.get_real_name()], c.get_real_name(), c.get_alias())
    elif dot_count == 2:
        dot_idx, _ = c.token_next_by(m=(T.Punctuation, '.'))
        one = c.tokens[dot_idx-1].value
        two = c.tokens[dot_idx+1].value
        three = c.tokens[dot_idx+3].value
        return ColumnInfo([one, two, three], three, c.get_alias())
    else:
        return ColumnInfo([None, None, c.get_real_name()], c.get_real_name(), c.get_alias())

def extract_table(parsed):
    tables = []
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                logger.info("is_subselect")
                #results = extract_table(item)
                pass
            elif item.ttype is Keyword:
                pass
            elif isinstance(item, Identifier):
                tables.append(handle_table_identifier(item))
            elif isinstance(item, IdentifierList):
                for child in item.get_identifiers():
                     tables.append(handle_table_identifier(child))

        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
    return tables

def handle_table_identifier(t):
    logger.info("======== handle_table_identifier ========")
    logger.info(t)
    logger.info(t.tokens)
    if isinstance(t.tokens[0], Parenthesis):
        sub = t.tokens[0]
        logger.info(sub)
        table_info = extract(sub, t.get_alias())
        return table_info
    else:
        logger.info(t.get_real_name())
        logger.info(t.get_alias())
        return TableInfo([t.get_parent_name(), t.get_real_name()], t.get_real_name(), t.get_alias())

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

class ColumnInfo:
    def __str__(self):
        return f"(full_name:{self.full_name}, name:{self.name}, alias:{self.alias})"

    def __init__(self, full_name, name, alias):
        self.full_name = full_name
        self.name = name
        self.alias = alias

    def result_name(self):
        if self.alias is not None:
            return self.alias
        else:
            return self.name

    def table_name(self):
        return self.full_name[1]

    def db_name(self):
        return self.full_name[0]


class TableInfo:
    def __str__(self):
        return f"(full_name:{self.full_name}, name:{self.name}, alias:{self.alias})"

    def print(self):
        logger.info(f"> full_name:{self.full_name}")
        logger.info(f"  name:{self.name}")
        logger.info(f"  alias:{self.alias}")

    def __init__(self, full_name, name, alias):
        self.full_name = full_name
        self.name = name
        self.alias = alias

    def result_name(self):
        if self.alias is not None:
            return self.alias
        else:
            return self.name

class TmpTableInfo:
    def __str__(self):
        return f"(full_name:{self.full_name}, name:{self.name}, alias:{self.alias}, columns: {self.columns}, context_tables:{self.context_tables})"

    def print(self):
        logger.info(f"> full_name:{self.full_name}")
        logger.info(f"  name:{self.name}")
        logger.info(f"  alias:{self.alias}")        
        logger.info(f"  columns:{self.columns}")
        for c in self.columns:
            logger.info(f"    column:{c}")
        logger.info(f"  context_tables:{self.context_tables}")
        for t in self.context_tables:
            t.print()

    def __init__(self, full_name, name, alias, columns, context_tables):
        self.full_name = full_name        
        self.name = name
        self.alias = alias
        self.columns = columns
        self.context_tables = context_tables

    def result_name(self):
        if self.alias is not None:
            return self.alias
        else:
            return self.name

def find_column_from(table_info, column):

    """输入的是一个表 table_info 和 其一个列 column
    返回列最终来自于那个数据库和表: [database, tablem, column], 找不到的返回 None
    """

    if column.full_name[0] != None and column.full_name[1] != None:
        return [column.full_name[0], column.full_name[1], column.full_name[2]]

    result_name = column.result_name()
    # table_name = column.table_name() if column.table_name() is not None else table_info.name
    table_name = column.table_name()
    name = column.name
    logger.info(f"result_name:{result_name}, name:{name}, table_name:{table_name}")

    context_tables = table_info.context_tables

    # column 来自于表
    target_table = None

    if table_name is None:  # table_name 为空，尝试从 context_tables 信息获得 target_table
        logger.info("table_name is None")
        if len(context_tables) == 1:
            target_table = context_tables[0]
        else:
            for t in context_tables:
                if isinstance(t, TmpTableInfo):
                    for c in t.columns:
                        if c.result_name() == name:
                            target_table = t
                            break
    else:   # table_name 不为空， 从 context_tables 遍历得到 target_table
        for t in context_tables:
            if t.result_name() == table_name:
                target_table = t
                break

    if target_table is None:
        logger.info("target_table is None")
        return [None, None, None]
    else:
        logger.info(f"target_table:{target_table}")
        if isinstance(target_table, TableInfo):
            return [target_table.full_name[0], target_table.full_name[1], column.name]
        else:
            col = None
            for c in target_table.columns:
                if c.result_name() == name:
                    col = c
                    break
            if col is None:
                return [None, None, None]
            else:
                return find_column_from(target_table, col)


if __name__ == '__main__':
    sql = """
select
  t1.a as ra,
  b as rb
from
  table1 as t1,
  (
    select
      table2.real_b as b,
      H.b as sb2,
      DB.H2.c2
    from
      table2,
      H
  ) t2,
  db2.table4 as t4
  join db3.table3 as t3
"""

    sql2 = """
select
  mobile
from(
    select
      mobile
    from
      dwd_crm_contact_relation_wide
  ) a
    """

    table_info = extract(sqlparse.parse(sql)[0])
    print("====== extract print======")
    table_info.print()
    print("====== extract print end ======")

    default_db_name = 'default_test_db'
    table_full_name = find_column_from(table_info, table_info.columns[1])
    if table_full_name[0] is None:
        table_full_name[0] = default_db_name
    print(table_full_name)
