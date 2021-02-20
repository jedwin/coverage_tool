# -*- coding: utf-8 -*-

"""
/***************************************************************************
 小区重叠计算工具，根据小区覆盖多边形数据表生成重叠关系数据表
                              -------------------
        begin                : 2020-01-14
        copyright            : (C) 2020 by Nathan Tse
        email                : xiezhuogang@gmail.com
 ***************************************************************************/

"""

import psycopg2
import time

arfcn_list = [75, 100, 1825, 1850, 2446, 2452]
tbl_name_coverage = 'cell_coverage_gd_202002'
'''
cell_coverage_gd_202002表格字段定义：
    city character varying,     地市
    enodeb_id integer,          不能为空
    cell_id integer,            不能为空
    arfcn integer,              不能为空
    pci integer,                可以为空
    geom geometry,              多边形空间数据字段
    area double precision,      小区覆盖面积，初始时没有该字段，在运行prepare_data()后自动生成
    latitude double precision,  小区纬度，可以为空
    longitude double precision  小区经度，可以为空
'''


class PostgresDb:
    db_host = 'localhost'
    db_name = 'postgres'
    db_user = 'postgres'
    db_password = 'postgres'
    db_port = 5432
    conn_str = ''
    conn = None
    engine_str = ''
    buffer_radius = 1000  # 人为将孤岛小区扩展的半径，单位：米。扩展越大越能保证孤岛站pci复用距离，但会使规划更困难
    isolate_cell_relation_thresh = 5  # 邻接小区数目小于该门限判断为孤岛小区
    isolate_cell_relation_factor = 0.01  # 为了减少扩展后的"虚拟"重叠面积过大，在计算扩展后的重叠面积时会乘上加此系数

    def __init__(self):
        self.conn = psycopg2.connect(host=self.db_host, port=5432, dbname=self.db_name,
                                     user=self.db_user,
                                     password=self.db_password)
        self.engine_str = 'postgresql://%s:%s@%s:%d/%s' % \
                          (self.db_user, self.db_password, self.db_host, self.db_port, self.db_name)

    def table_is_exist(self, table_name):
        """
        检查输入表是否在数据库中存在
        :param table_name: 表名
        :return: True/False
        """
        try:
            cur = self.conn.cursor()
            cur.execute(f'select count(*) from pg_class where relname = \'{table_name}\';')
            table_count = cur.fetchone()
            # table_count should be a tuple
            if table_count[0] > 0:
                return True
            else:
                return False
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()

    def test(self):
        """
        通过检查数据库版本，验证连接是否正常
        :return: True or False
        """
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            return db_version
            cur.close()

            return True
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()
                # print('Cursor is closed.')

    def close(self):
        if self.conn is not None:
            self.conn.close()
            print('Database connection closed.')

    def prepare_data(self, table_name, geom_idx='geom_idx'):
        """
        数据预处理：
        1、增加面积字段、geom2临时字段
        2、makevalid可能出现异常的geometry，然后在统一转换为Multi Polygon格式
        3、创建索引
        4、计算每个小区覆盖面积
        :param table_name:  指定小区多边形表名
        :param geom_idx:    指定索引名称，以免与数据库内其他索引冲突
        :return:            失败时返回False
        """
        try:
            cur = self.conn.cursor()
            if len(table_name) > 0:
                if self.table_is_exist(table_name):
                    print(f'开始对{table_name}进行预处理')
                    print(f'adding geom2 to {table_name}')
                    # 增加面积字段、geom2临时字段
                    cur.execute(f'ALTER TABLE {table_name} ADD COLUMN geom2 geometry;')
                    cur.execute('commit;')
                    # 将可能出现异常的geometry makevalid，然后在统一转换为Multi Polygon格式。
                    print(f'开始处理异常多边形，可能需要好几分钟……')
                    cur.execute(f'update {table_name} Set geom2 =  ST_CollectionExtract((st_makevalid(geom)),3);')
                    cur.execute('commit;')
                    cur.execute(f'ALTER TABLE {table_name} DROP COLUMN geom;')
                    cur.execute(f'ALTER TABLE {table_name} RENAME geom2 TO geom;')
                    cur.execute('commit;')
                    print(f'开始创建空间索引')
                    cur.execute(f'drop index {geom_idx};')
                    cur.execute(f'CREATE INDEX {geom_idx} ON {table_name} USING GIST (geom);')
                    cur.execute(f'ALTER TABLE {table_name} ALTER COLUMN geom SET NOT NULL;')
                    cur.execute('commit;')
                    print('计算每个小区覆盖面积')
                    cur.execute(f'ALTER TABLE {table_name} ADD COLUMN area double precision;')
                    cur.execute(f'update {table_name} Set area = st_area(geom::geography);')
                    cur.execute('commit;')
                else:
                    print(f'表：{table_name} 不存在')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()

    def generate_relation(self, table_name, relation_table, cities, arfcn=[], extend_cell=False):
        """
        用于生成每个小区的邻接重叠小区关系表

        :param table_name: 小区覆盖数据表名
        :param relation_table: 重叠关系表名
        :param cities: 用于生成重叠关系的地市
        :param arfcn: 指定生成重叠关系的频点列表，如果留空，则按arfcn_list
        :param extend_cell: 是否人为对孤岛小区面积进行扩展，True/False
        :return:
        """
        try:
            cur = self.conn.cursor()
            if len(table_name) > 0:
                if self.table_is_exist(table_name):
                    print(f'开始对{table_name}计算重叠关系')
                    if len(cities) == 0:
                        return False
                    t1 = time.time()
                    for city in cities:
                        print(f'清空{city}地市的记录')
                        cur.execute(f'delete from {relation_table} where city=\'{city}\';')
                        cur.execute('commit;')
                        if len(arfcn) == 0:
                            arfcn = arfcn_list
                        arfcn_1 = arfcn.copy()
                        arfcn_2 = arfcn.copy()
                        for a_arfcn in arfcn_1:
                            for b_arfcn in arfcn_2:
                                select_clause = f'''
                                select a.city, a.enodeb_id, a.cell_id, a.arfcn, a.area,
                                a.pci, b.city b_city, b.enodeb_id b_enodeb_id, b.cell_id b_cell_id,
                                b.arfcn b_arfcn, b.area b_area, b.pci b_pci,
                                st_area((st_intersection(a.geom, b.geom))::geography), false expanded'''

                                from_clause = f''' from {table_name} a, {table_name} b'''

                                where_clause = f''' where (a.geom && b.geom)
                                and (a.enodeb_id != b.enodeb_id or a.cell_id != b.cell_id)
                                and a.arfcn = {a_arfcn}
                                and b.arfcn = {b_arfcn}
                                and a.city = \'{city}\';'''
                                # and (b.city=a.city or b.city in ({str(city_relation[city])[1:-1]}))
                                if self.table_is_exist(relation_table):
                                    sql_string = (f'''insert into {relation_table}'''
                                                  + select_clause + from_clause + where_clause)
                                else:
                                    sql_string = (select_clause
                                                  + f''' into {relation_table}''' + from_clause + where_clause)

                                print(f'正在计算{city}的{a_arfcn}和{b_arfcn}重叠关系')

                                cur.execute(sql_string)
                                cur.execute('commit;')
                            # 未避免重复计算，每次b_arfcn循环完之后，就删除已计算过的频点
                            arfcn_2.remove(a_arfcn)

                        if extend_cell:
                            # 开始扩展孤岛小区
                            print(f'开始扩展{city}的孤岛小区')
                            sql_string = f'''
                            with isolate_cells as (
                            select b.city, b.enodeb_id, b.cell_id, b.arfcn,b.area, b.pci,st_setsrid(st_buffer(
                            b.geom::geography,{self.buffer_radius})::geometry,4326) geom, count(*) relations 
                            from {relation_table} a 
                            right join {table_name} b on (a.enodeb_id=b.enodeb_id and a.cell_id=b.cell_id)
                            where b.city=\'{city}\'
                            group by b.city, b.enodeb_id, b.cell_id,b.arfcn,b.area,b.pci, b.geom
                            having count(*) < {self.isolate_cell_relation_thresh}
                            order by count(*))
    
                            insert into {relation_table}
                            select a.city, a.enodeb_id, a.cell_id, a.arfcn, a.area, a.pci, b.city b_city, b.enodeb_id 
                            b_enodeb_id, b.cell_id b_cell_id, b.arfcn b_arfcn, b.area b_area, b.pci b_pci, 
                            st_area((st_intersection(a.geom, b.geom))::geography) * {self.isolate_cell_relation_factor},
                            true expanded 
                            from isolate_cells a, {table_name} b
                            where (a.geom && b.geom)
                            and (a.enodeb_id != b.enodeb_id or a.cell_id != b.cell_id);
                            '''
                            cur.execute(sql_string)
                            cur.execute('commit;')
                            print('删除重叠面积为0的记录')
                            cur.execute(f'delete from {relation_table} where st_area=0;')
                            cur.execute('commit;')
                    t2 = time.time()
                    print(f'共耗时{t2 - t1}秒')
                else:
                    print(f'表：{table_name} 不存在')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return False
        finally:
            if cur is not None:
                cur.close()


if __name__ == '__main--':
    # 运行顺序
    # mydb.prepare_data(tbl_name_coverage, geom_idx='geom_idx_202002')
    # mydb.generate_relation(tbl_name_coverage, tb_name_coverage_relation,
    #                        ['CZ', 'DG', 'FS', 'GZ', 'HY', 'HZ', 'JM', 'JY', 'MM', 'MZ', 'QY', 'SG',
    #                         'ST', 'SW', 'SZ', 'YF', 'YJ', 'ZH', 'ZJ', 'ZQ', 'ZS'])
    pass
