import db_connector
import lte_coverage
import time
import logging
import math
import pandas as pd
import csv
import xlrd

Data_Type_Both = 'Both'
Data_Type_HO = 'Handover'
Data_Type_Coverage = 'Coverage'

distance_thresh = 2000  # 复用距离门限
missing_cells = []
MAX_PCI_LTE = 503  # PCI最大值
MAX_SSS = 167  # sss最大值
display_mod_num = 300  # 每规划多少个显示一次消耗时间
cells_to_plan_list = []  # 需要规划的小区列表
groups_to_plan_list = []  # 需要规划的小区组列表
cells = lte_coverage.cells  # {enodeb_id_cell_id: LteCell}
enodebs = lte_coverage.enodebs  # {enodeb_id: enodeb}
pci_count = {}  # {arfcn_pci: reuse_count}
relations = {}
plan_result = {}  # 存放规划结果
result_file = 'pci_planning_result.csv'
before_file = 'pci_planning_before_1.csv'
logging.basicConfig(level=logging.ERROR, format='', filename=lte_coverage.log_file, filemode='w')
logger = logging.getLogger('main')


def get_cells(cities=[], sql_string='', auto_neighbor=True):
    """
    从数据库中导入小区数据，生成cell、bbu对象，放入cells{}
    :param auto_neighbor:
    :param sql_string:
    :param cities:
    :return: 成功生成小区数据，返回True，否则返回False
    """
    mydb = db_connector.PostgresDb()
    global enodebs
    global cells
    global logger

    if mydb.test():
        try:
            cells_data = mydb.get_cells(cities=cities, sql_string=sql_string, auto_neighbor=auto_neighbor)
            if not cells_data:
                print('Failed to get cell data')
                return False
            '''
            columns:
             0   city 
             1   enodeb_id 
             2   cell_id 
             3   arfcn 
             4   pci 
             5   null 
             6   area 
             7   can_be_changed
             8   longitude 不一定有
             9   latitude  不一定有
            '''
            print(f'There are {len(cells_data)} records')
            duplicate_list = []
            for rec in cells_data:
                #  检查city配置和字段
                city = rec[0]
                # 检查关键字段是否合规
                if rec[1] > 0 and rec[2] >= 0 and rec[3] > 0 and rec[4] >= -1:
                    enodeb_id = rec[1]
                    cell_id = rec[2]
                    arfcn = rec[3]
                    # 统一1.8G和2.1G频点号
                    if arfcn == 1825:
                        arfcn = 1850
                    elif arfcn == 75:
                        arfcn = 100
                    pci = rec[4]
                else:
                    logger.error(f'{rec[1]}_{rec[2]}: arfcn:{rec[3]}, pci:{rec[4]}')
                if rec[6]:
                    area = rec[6]
                else:
                    area = 0

                # 注意：经纬度可能为None
                longitude = rec[8]
                latitude = rec[9]

                # 检查can_be_changed字段
                if rec[7] is None:
                    pci_can_be_changed = True
                else:
                    pci_can_be_changed = rec[7]

                # 创建enodeb对象，enodeb对象必须早于cell对象建立，才能正确设置cell对象中的parent对象
                if enodeb_id not in enodebs.keys():
                    enodebs[enodeb_id] = lte_coverage.enodeb(city=city, enodeb_id=enodeb_id)

                # 创建cell对象
                cell = lte_coverage.LteCell(city=city, enodeb_id=enodeb_id, cell_id=cell_id,
                                            arfcn=arfcn, pci=pci, area=area, can_be_changed=pci_can_be_changed)
                cell.parent = enodebs[enodeb_id]
                if longitude is not None and latitude is not None:
                    cell.set_location(latitude, longitude)
                if cell.index not in cells.keys():
                    cells[cell.index] = cell
                    # 将cell对象加入enodeb的cell列表
                    enodebs[enodeb_id].cells[cell.index] = cell
                    if city not in cell.city_arfcn_pci_count.keys():
                        cell.city_arfcn_pci_count[city] = {}
                        cell.city_arfcn_sss_count[city] = {}
                    if arfcn not in cell.city_arfcn_pci_count[city].keys():
                        cell.city_arfcn_pci_count[city][arfcn] = {}
                        cell.city_arfcn_sss_count[city][arfcn] = {}
                    if pci not in cell.city_arfcn_pci_count[city][arfcn].keys():
                        cell.city_arfcn_pci_count[city][arfcn][pci] = 1
                    else:
                        cell.city_arfcn_pci_count[city][arfcn][pci] += 1
                    sss = math.floor(pci / 3)
                    if sss not in cell.city_arfcn_sss_count[city][arfcn].keys():
                        cell.city_arfcn_sss_count[city][arfcn][sss] = 1
                    else:
                        cell.city_arfcn_sss_count[city][arfcn][sss] += 1
                else:
                    duplicate_list.append(cell.index)

            # 补全city_arfcn_pci_count里面的所有频点的所有PCI和SSS次数统计
            for city in cell.city_arfcn_pci_count.keys():
                for arfcn in cell.city_arfcn_pci_count[city].keys():
                    for pci in range(lte_coverage.MAX_PCI_LTE + 1):
                        sss = math.floor(pci / 3)
                        if pci not in cell.city_arfcn_pci_count[city][arfcn].keys():
                            cell.city_arfcn_pci_count[city][arfcn][pci] = 0
                        if sss not in cell.city_arfcn_sss_count[city][arfcn].keys():
                            cell.city_arfcn_sss_count[city][arfcn][sss] = 0
            print(f'There are {len(duplicate_list)} duplicate cell records. Number of cells: {len(cells)}')
            return True
        except:
            print(rec)
    else:
        print('failed to connect postgres')
        return False


def get_relations(cities=[], relation_type=Data_Type_Coverage, auto_neighbor=True):
    global enodebs
    global cells
    global relations
    global logger

    mydb = db_connector.PostgresDb()
    if mydb.test():
        cells_data = mydb.get_relation(cities=cities, relation_type=relation_type, auto_neighbor=auto_neighbor)
        i = 1
        '''
        Columns          
         0   enodeb_id 
         1   cell_id 
         2   b_enodeb_id 
         3   b_cell_id 
         4   relation 

        '''
        print(f'There are {len(cells_data)} relation records')
        for rec in cells_data:
            cell_a_index = '%d_%d' % (rec[0], rec[1])
            cell_b_index = '%d_%d' % (rec[2], rec[3])

            overlaps_area = rec[4]

            relation_index_1 = '%s|%s' % (cell_a_index, cell_b_index)
            relation_index_2 = '%s|%s' % (cell_b_index, cell_a_index)
            if cell_a_index not in cells.keys():
                missing_cells.append(cell_a_index)
                logger.warning(f'missing cella in {relation_type}: {rec}')
            elif cell_b_index not in cells.keys():
                missing_cells.append(cell_b_index)
                logger.warning(f'missing cellb in {relation_type}: {rec}')
            else:
                cell_a = cells[cell_a_index]
                cell_b = cells[cell_b_index]

                if cell_b_index not in cell_a.adjacent_cells.keys():
                    # 如果B没在A的邻区关系中，则加入
                    # if relation_type == Data_Type_Coverage:
                    cell_a.add_adj_cell(cell_b, overlaps_area)

                    distance = 0  # cell_a.distance(cell_b)
                    relations[relation_index_1] = \
                        lte_coverage.Relation(cell_a_index, cell_b_index, distance, overlaps_area)

                if cell_a_index not in cell_b.adjacent_cells.keys():
                    # 确保双向添加
                    cell_b.add_adj_cell(cell_a, overlaps_area)
                    relations[relation_index_2] = \
                        lte_coverage.Relation(cell_b_index, cell_a_index, distance, overlaps_area)

        print(f'missing_cells: {len(set(missing_cells))}. Number of relations: {len(relations)}')
        if len(set(missing_cells)) > 0:
            print(set(missing_cells))

    else:
        print('failed to connect postgres')


def get_2way(city, arfcn_list=[]):
    pci_conflict_dict = {}
    pci_conflict_dict_final = {}
    # 让每个小区按arfcn_pci分类和自己有重叠覆盖的邻区，将
    for pCell in cells.values():
        pCell.get_arfcn_pci_dict(city=city, arfcn_list=arfcn_list)
        pci_conflict_dict.update(pCell.adjacent_arfcn_pci)

    # 统计所有小区按arfcn_pci分类后的邻区个数，超过1时就表示存在2way情况
    # 将这些超过1的pci混淆小区对放入pci_conflict_dict_final字典

    for cell_pci_index in pci_conflict_dict.keys():
        if len(pci_conflict_dict[cell_pci_index]) > 1:
            # 注意，这里出来的列表，可能包括2个或以上的小区
            pci_conflict_dict_final[cell_pci_index] = pci_conflict_dict[cell_pci_index]
    return pci_conflict_dict_final


def show_rank(i=''):
    if i == '':
        for cell in cells.values():
            cell.pci_ranking()
            print('%s,%s' % (cell.index, len(cell.pci_rank)))
    else:
        cells[i].pci_ranking()
        print(cells[i].index, cells[i].mod3_rank)
        print(cells[i].index, 'All PCI ranking:')
        for pci in cells[i].pci_rank.keys():
            print(pci, cells[i].pci_rank[pci])


def get_available_sss():
    """
    为每个bbu生成可以用sss字典colocation_group_available_sss
    {group_id: [sss1, sss2...]}
    :return: 成功返回True，否则返回False
    """

    for my_bbu_id, my_bbu in enodebs.items():
        for group_id, group in my_bbu.colocation_cells_groups.items():
            available_sss = set(range(0, lte_coverage.MAX_SSS + 1))
            for this_cell in group:
                available_sss_in_this_cell = set(math.floor(i / 3) for i in this_cell.available_pci)
                available_sss.intersection(available_sss_in_this_cell)
            my_bbu.colocation_group_available_sss[group_id] = available_sss
    return True


def least_available_sss_bbu_group(bbu_group_list, used_sss, used_bbu_group_id):
    """
    在输入的bbu_group列表中选取sss受限最多（可选最少）的那个，以便用来规划
    :param used_sss:
    :param bbu_group_list: bbu_group_id列表
    :return: bbu_group_id
    """
    available_sss = lte_coverage.MAX_SSS + 2
    ret_obj = ('', 0)
    next_id = ''
    used_bbu_id, used_group_id = used_bbu_group_id.split('_')
    used_bbu_id = int(used_bbu_id)
    used_group_id = int(used_group_id)
    for bbu_group_id, count in bbu_group_list:
        bbu_index, group_id = bbu_group_id.split('_')
        bbu_index = int(bbu_index)
        group_id = int(group_id)
        my_bbu = enodebs[bbu_index]
        if used_sss in my_bbu.colocation_group_available_sss[group_id] \
                and (used_bbu_id in my_bbu.adjacent_enodeb[group_id]
                     or used_bbu_id in my_bbu.second_tier_enodeb[group_id]):
            my_bbu.colocation_group_available_sss[group_id].remove(used_sss)
        remaining_sss_count = len(my_bbu.colocation_group_available_sss[group_id])
        if available_sss > remaining_sss_count:
            available_sss = remaining_sss_count
            next_id = bbu_group_id
            ret_obj = (bbu_group_id, count)

    # 开始对下一个即将要规划的bbu_group_id的影响进行评估
    bbu_index, group_id = next_id.split('_')
    bbu_index = int(bbu_index)
    group_id = int(group_id)
    next_bbu = enodebs[bbu_index]
    remaining_sss = {}
    for other_bbu_group_id, count in bbu_group_list:
        other_bbu_index, _ = other_bbu_group_id.split('_')
        other_bbu_index = int(other_bbu_index)

        if (other_bbu_index in next_bbu.adjacent_enodeb[group_id]
                or other_bbu_index in next_bbu.second_tier_enodeb[group_id]):
            for other_group_id in enodebs[other_bbu_index].colocation_cells_groups.keys():
                for sss in enodebs[other_bbu_index].colocation_group_available_sss[other_group_id]:
                    if sss in remaining_sss.keys():
                        remaining_sss[sss] += 1
                    else:
                        remaining_sss[sss] = 1
    # logger.info(f'ret_id: {ret_obj}, available_sss:{available_sss}, remaining_sss:{remaining_sss}')
    return [ret_obj, [i[0] for i in lte_coverage.sort_value_in_dict(remaining_sss, reverse=True)]]
    # return [ret_obj, list(remaining_sss.keys())]


def plan_pci(checking_cities=[], arfcn_list=[], data_type=Data_Type_Both, plan_2way=False, restrict_sss=None,
             top_big_cells=0, top_most_pci=0, reset_pci=False, export_all=False, try_3_tier=True,
             must_equal_zero=True, from_less_sss=False, restrict_pci=[], auto_neighbor=True):
    """
    对指定地市规划PCI，解决PCI冲突或混淆问题
    :param restrict_sss:
    :param try_3_tier:
    :param auto_neighbor: 是否自动获取相邻地市
    :param restrict_pci: 禁止使用的PCI列表
    :param from_less_sss: 是否从最小的SSS开始尝试分配
    :param must_equal_zero: 是否必须让混淆条数为0
    :param plan_2way: 是否针对混淆的小区对做重规划
    :param data_type:
        Data_Type_Both:同时使用切换和覆盖数据，
        Data_Type_HO：只使用切换数据，
        Data_Type_Coverage：只使用覆盖数据
    :param reset_pci:       True: 将所有涉及规划地市和频点的小区PCI先重置为0，再统一规划
    :param checking_cities: 需要规划PCI的地市列表，例如['ST', 'FS']，如果不指定，就对数据库中全部地市进行规划
    :param arfcn_list:      需要规划PCI的频点列表，例如[1825, 100]，如果不指定，就对数据库中全部频点进行规划
    :param top_big_cells:   在计算混淆前，先对覆盖范围（邻接小区数量）最多的小区进行重规划的个数，不管它们是否存在PCI混淆问题
    :param top_most_pci: 在计算混淆前，先对PCI复用次数最多的小区进行重规划的PCI个数，不管它们是否存在PCI混淆问题
    :param export_all:      True：导出指定地市全部小区，False：只导出PCI有变更的小区。导出结果记录在logger中
    :return:                正常完成规划返回True，其他情况返回False
    """
    priority_arfcn = arfcn_list
    # 开始计时
    t1 = time.time()
    if restrict_pci is None:
        restrict_pci = []
    if restrict_sss is None:
        restrict_sss = set()
    restrict_sss = restrict_sss.union(set([math.floor(i / 3) for i in restrict_pci]))
    sql_string = ''
    print(f'restrict_sss: {restrict_sss}')
    if not get_cells(cities=checking_cities, auto_neighbor=auto_neighbor):
        print(f'failed in getting cells.')
        exit(1)
    # 读取两种关系数据，确保不遗漏
    if data_type == Data_Type_Both or data_type == Data_Type_Coverage:
        get_relations(cities=checking_cities, relation_type=Data_Type_Coverage, auto_neighbor=auto_neighbor)
    if data_type == Data_Type_Both or data_type == Data_Type_HO:
        get_relations(cities=checking_cities, relation_type=Data_Type_HO, auto_neighbor=auto_neighbor)
    plan_result = {}
    cells_to_plan_list = []
    for bbu in enodebs.values():
        bbu.detect_same_location(arfcn_list=arfcn_list)

    for city in checking_cities:
        # 先将涉及到的小区放入cells_to_plan列表
        for cell in cells.values():
            if (len(arfcn_list) == 0 or cell.arfcn in arfcn_list) \
                    and cell.city == city:
                cells_to_plan_list.append(cell)
        print(f'There are {len(cells_to_plan_list)} cells in {arfcn_list} need to be planned.')
        # 进行重置PCI判断
        reset_time_begin = time.time()
        if reset_pci:
            print('Resetting PCI...')

            top_most_pci = 0
            for cell in cells_to_plan_list:
                cell.pci = -1
                cell.available_pci = set(range(MAX_PCI_LTE + 1))
                cell.available_pci_3_tier = set(range(MAX_PCI_LTE + 1))

            # 更新city_arfcn_pci_count和city_arfcn_sss_count类属性
            for arfcn in arfcn_list:
                if arfcn in lte_coverage.LteCell.city_arfcn_pci_count[city].keys():
                    for pci in range(MAX_PCI_LTE + 1):
                        lte_coverage.LteCell.city_arfcn_pci_count[city][arfcn][pci] = 0
                    for sss in range(MAX_SSS + 1):
                        lte_coverage.LteCell.city_arfcn_sss_count[city][arfcn][sss] = 0
        # logger.debug(lte_coverage.LteCell.city_arfcn_pci_count)
        # logger.debug(f'after reset: {lte_coverage.LteCell.city_arfcn_sss_count[city][1850]}')
        # exit(1)
        print(f'Generating bbu and cells tier info in {city}...')
        lte_coverage.generate_all_tiers(city, arfcn_list)
        # exit(1)
        reset_time_end = time.time()
        print(f'PCI reset in {int(reset_time_end - reset_time_begin)} seconds')
        # print('Counting available sss...')
        # get_available_sss()

        # 将该地市覆盖小区最多的n个小区重规划一遍
        if top_big_cells > 0:
            i = 0
            planned_bbu = []
            # top_big_cells = min(len(lte_coverage.cell_size_ladder), top_big_cells)
            top_big_cells = min(len(lte_coverage.enodeb_size_ladder), top_big_cells)
            # print(f'There are {len(lte_coverage.cell_size_ladder)} cells, planning for Top {top_big_cells}')
            print(f'There are {len(lte_coverage.enodeb_size_ladder)} cell groups, planning for Top {top_big_cells}')
            # for big_cell_index, _ in lte_coverage.cell_size_ladder[:top_big_cells]:
            t2 = time.time()
            planning_bbu = lte_coverage.enodeb_size_ladder[:top_big_cells]
            # used_sss = lte_coverage.MAX_SSS + 1
            # used_bbu_group_id = '0_0'
            for i in range(0, top_big_cells):

                # bbu_colocation_group, remaining_sss = least_available_sss_bbu_group(planning_bbu,
                #                                                                     used_sss, used_bbu_group_id)
                # big_bbu_index, group_id = bbu_colocation_group[0].split('_')
                bbu_colocation_group = planning_bbu[i]
                big_bbu_index, group_id = bbu_colocation_group[0].split('_')
                big_bbu = enodebs[int(big_bbu_index)]
                # remaining_sss = []
                if i % 100 == 0 and i > 0:
                    t3 = time.time()
                    print(f'{i} / {top_big_cells} {int(t3 - t2)}s')
                    t2 = t3
                planned_bbu.append(big_bbu_index)
                # planning_bbu.remove(bbu_colocation_group)
                planned_bbu_result = big_bbu.pick_pci(try_3_tier=try_3_tier,
                                                      restrict_group=True, restrict_sss=restrict_sss,
                                                      must_equal_zero=must_equal_zero, from_less_sss=from_less_sss,
                                                      colocation_cells_group_id=int(group_id))
                if not planned_bbu_result:
                    break
                # used_sss = math.floor(list(planned_bbu_result.values())[0] / 3)
                # used_bbu_group_id = bbu_colocation_group[0]
                plan_result.update(planned_bbu_result)

        # 对复用次数最多的n个pci的所有小区进行重规划
        if top_most_pci > 0:
            print('Planning for Top %d reuse PCI cells in %s...' % (top_most_pci, city))
            # 因为city_arfcn_pci_count是对象属性，所以取第一个小区对象即可
            city_arfcn_pci_count = cells_to_plan_list[0].city_arfcn_pci_count
            for arfcn in arfcn_list:
                most_pci_list = [i[0] for i in lte_coverage.sort_value_in_dict(city_arfcn_pci_count[city][arfcn],
                                                                               reverse=True)[:top_most_pci]]
                planned_bbu = []
                for cell in cells_to_plan_list:
                    if cell.city == city and cell.arfcn == arfcn and cell.pci in most_pci_list:
                        bbu = cell.parent
                        if bbu.enodeb_id not in planned_bbu:
                            planned_bbu.append(bbu.enodeb_id)
                            planned_bbu_result = bbu.pick_pci(arfcn_list=arfcn_list,
                                                              restrict_group=True, restrict_pci=restrict_pci,
                                                              must_equal_zero=must_equal_zero,
                                                              from_less_sss=from_less_sss,
                                                              colocation_cells_group_id=int(group_id))
                            if not planned_bbu_result:
                                break
                            plan_result.update(planned_bbu_result)

        # 开始对现有2way小区进行重规划
        # pci_conflict_list = lte_coverage.sort_value_in_dict(get_2way(city, arfcn_list), reverse=True)
        pci_conflict_list = get_2way(city, arfcn_list).values()
        print('length of pci_conflict_list:', len(pci_conflict_list))

        for item in pci_conflict_list:
            print(item)

        if plan_2way:
            remaining_2way = len(pci_conflict_list) + 1  # 之所以要加1是为了启动第一次规划循环
            # 只要2way数量比上次有减少，就继续规划
            while remaining_2way > len(pci_conflict_list):
                print('length of pci_conflict_list:', len(pci_conflict_list))

                remaining_2way = len(pci_conflict_list)

                planned_bbu = []
                i = 0
                for cell_index_list in pci_conflict_list:
                    i += 1
                    if i % 100 == 0:
                        print(f'{i} / {remaining_2way}')
                    for cell_index in cell_index_list:
                        cell = cells[cell_index]
                        if (len(arfcn_list) == 0 or cell.arfcn in arfcn_list) and cell.city == city:
                            bbu = cell.parent
                            for group_id, bbu_colocation_group in bbu.colocation_cells_groups.items():
                                if cell in bbu_colocation_group:
                                    bbu_group_id = '%d_%d' % (bbu.enodeb_id, group_id)
                                    if bbu_group_id not in planned_bbu:
                                        planned_bbu.append(bbu_group_id)
                                        planned_bbu_result = bbu.pick_pci(arfcn_list=arfcn_list,
                                                                          restrict_group=True,
                                                                          restrict_pci=restrict_pci,
                                                                          must_equal_zero=must_equal_zero,
                                                                          from_less_sss=from_less_sss,
                                                                          colocation_cells_group_id=int(group_id))
                                        plan_result.update(planned_bbu_result)
                # pci_conflict_list = lte_coverage.sort_value_in_dict(get_2way(city, arfcn_list), reverse=True)
                pci_conflict_list = get_2way(city, arfcn_list).values()
                if len(pci_conflict_list) == 0 or len(pci_conflict_list) >= remaining_2way:
                    break

        print('exporting results')
        if export_all:
            export_cells(checking_cities, arfcn_list)
        else:
            logger.error('enodeb_id, cell_id, pci, can_be_changed, plan_in_tiers')
            for key, val in plan_result.items():
                enodeb_id = int(key.split('_')[0])
                cell_id = int(key.split('_')[1])
                cell = cells[key]
                logger.error('%d,%d,%d,%s,%d' % (enodeb_id, cell_id, val, True, cell.pci_plan_in_tiers))

        t5 = time.time()
        print('Planning %d cell groups in %d seconds:' % (len(plan_result), int(t5 - t1)))
        return True


def plan_pci2(checking_cities=[], arfcn_list=[], top_big_cells=False, reset_pci=True, restrict_sss=None
              , try_3_tier=False, plan_type=3, must_equal_zero=True, assign_3_tier=False):
    """
    规划PCI
    :param assign_3_tier: 当设置为True时，每配置一个group，就将其3层小区能使用相同sss的group都配置上
    :param must_equal_zero: 当设置为True时，2way必须为0，如果无法找到合适的PCI就终止规划
    :param checking_cities:
    :param arfcn_list:
    :param top_big_cells:
    :param reset_pci:
    :param restrict_sss:
    :param try_3_tier:
    :param plan_type:
        0:  # 尽量用排在前面的pci
        1:  # 尽量集中使用pci
        2:  # 尽量分散使用pci
        3:  # 尽量选用对后续规划影响少的sss，其次尽量分散，最后再尽量使用靠前的序号
        4:  # 尽量选用对后续规划影响少的sss，其次尽量集中，最后再尽量使用靠前的序号
        5： # 优先使用第3层小区已使用的sss，其次尽量集中，最后再尽量使用靠前的序号
    :return:
    """
    # 重置需要规划小区的PCI=-1

    if restrict_sss is None:
        restrict_sss = set()
    if reset_pci:
        print('Resetting PCI...')
        plan_result.clear()
        reset_time_begin = time.time()
        for city in checking_cities:
            top_most_pci = 0
            for cell in cells_to_plan_list:
                if cell.can_be_changed:
                    cell.pci = -1
                    cell.available_pci = set(range(MAX_PCI_LTE + 1))
                    cell.available_pci_3_tier = set(range(MAX_PCI_LTE + 1))

            # 更新city_arfcn_pci_count和city_arfcn_sss_count类属性
            for arfcn in arfcn_list:
                if arfcn in lte_coverage.LteCell.city_arfcn_pci_count[city].keys():
                    for pci in range(MAX_PCI_LTE + 1):
                        lte_coverage.LteCell.city_arfcn_pci_count[city][arfcn][pci] = 0
                    for sss in range(MAX_SSS + 1):
                        lte_coverage.LteCell.city_arfcn_sss_count[city][arfcn][sss] = 0
        # tmp_groups_to_plan_list = groups_to_plan_list
        # 因为重置PCI会令所有规划小区的可用PCI全部恢复，因此需要针对边界小区进行删减可用PCI
        print(f'Generating bbu and cells tier info in {city}...')
        lte_coverage.generate_all_tiers(city, arfcn_list)
        reset_time_end = time.time()
        print(f'PCI reset in {int(reset_time_end - reset_time_begin)} seconds')

    if top_big_cells:
        # 对覆盖最多小区的前top_big_cells个进行pci规划
        t1 = time.time()
        i = 0
        planned_bbu = []
        planned_group = []
        num_of_bbu_group = len(groups_to_plan_list)
        print(f'There are {num_of_bbu_group} cell groups')

        t2 = time.time()
        series_enodeb_group_size = lte_coverage.series_enodeb_group_size
        enodeb_group_sss_size_dict = {}
        # print(f'Generating available sss for every group in planning list')
        for group in groups_to_plan_list:
            group.generate_available_sss()
            enodeb_group_sss_size_dict[group.index] = len(group.available_sss)
        series_enodeb_group_sss_size = pd.Series(enodeb_group_sss_size_dict)
        df_planning_group = pd.DataFrame()

        df_planning_group['neighbor_size'] = series_enodeb_group_size
        df_planning_group['available_sss_size'] = series_enodeb_group_sss_size

        df_planning_group = df_planning_group.dropna(axis=0)

        planning_bbu_group = list(df_planning_group.sort_values(by=['available_sss_size', 'neighbor_size']
                                                                , ascending=[True, False]).index)
        i = 0
        j = 0
        for m in range(num_of_bbu_group):
            # 只对排在最优先的组进行规划
            # print(f'len of planning_bbu_group: {len(planning_bbu_group)}')
            bbu_group_index = planning_bbu_group[0]
            big_bbu_index, group_id = bbu_group_index.split('_')
            big_bbu = enodebs[int(big_bbu_index)]
            group_id = int(group_id)
            bbu_group = big_bbu.colocation_cells_groups[group_id]
            # if bbu_group not in planned_group:
            # print(f'planning: {bbu_group_index}, m={m}, num_of_bbu_group={num_of_bbu_group}')
            # print(f'planning_bbu_group: {planning_bbu_group}')
            planned_group.append(bbu_group)
            planning_bbu_group.remove(bbu_group_index)
            series_enodeb_group_size = series_enodeb_group_size.drop(labels=bbu_group_index)
            i += 1
            if (i + j) % display_mod_num == 0:
                t3 = time.time()
                if assign_3_tier:
                    print(f'{i} + {j} of {num_of_bbu_group} {int(t3 - t2)}s')
                else:
                    print(f'{i} of {num_of_bbu_group} {int(t3 - t2)}s')

                t2 = t3
                # print(df_planning_group)
            planned_bbu_result = bbu_group.pick_pci(try_3_tier=try_3_tier, restrict_sss=restrict_sss,
                                                    sort_type=plan_type, must_equal_zero=must_equal_zero)

            if planned_bbu_result:
                plan_result.update(planned_bbu_result)
                enodeb_group_sss_size_dict = {}
                for group_index in planning_bbu_group:
                    bbu_index, group_id = group_index.split('_')
                    group = enodebs[int(bbu_index)].colocation_cells_groups[int(group_id)]
                    group.generate_available_sss()
                    enodeb_group_sss_size_dict[group_index] = len(group.available_sss)
                series_enodeb_group_sss_size = pd.Series(enodeb_group_sss_size_dict)
                df_planning_group = pd.DataFrame()
                df_planning_group['neighbor_size'] = series_enodeb_group_size
                df_planning_group['available_sss_size'] = series_enodeb_group_sss_size
                # df_planning_group.fillna(9999)
                planning_bbu_group = list(df_planning_group.sort_values(by=['available_sss_size', 'neighbor_size']
                                                                        , ascending=[True, False]).index)
                if assign_3_tier:
                    assigned_sss = math.floor(list(planned_bbu_result.values())[0] / 3)
                    k = 0
                    # 开始对每个3层邻区进行遍历
                    for third_tier_group in group.third_tier_groups:
                        # 每次最多只对n个3层小区组进行分配
                        if k >= 100:
                            break
                        # third_tier_group = cells[third_tier_cell_index]

                        if third_tier_group in groups_to_plan_list:

                            third_tier_cell_bbu = third_tier_group.parent
                            third_tier_cell_group_id = third_tier_group.group_id
                            third_tier_bbu_index = f'{third_tier_group.index}'
                            if third_tier_bbu_index not in planned_bbu:
                                cell_list = third_tier_group.cell_list
                                #  直接尝试分配指定sss
                                planned_bbu_result = lte_coverage.try_assign_sss_to_group(sss_list=[assigned_sss],
                                                                                          cell_list=cell_list)
                                if planned_bbu_result:
                                    plan_result.update(planned_bbu_result)
                                    planned_bbu.append(third_tier_bbu_index)
                                    j += 1
                                    k += 1
                                    m += 1
                                    if (i + j) % display_mod_num == 0:
                                        t3 = time.time()
                                        print(f'{i} + {j} of {num_of_bbu_group} {int(t3 - t2)}s')
                                        # print(f'planned_bbu inside :{planned_bbu}')
                                        t2 = t3
            else:
                if len(bbu_group.cell_list) > 1:
                    print(f'Trying assign pci for group: {bbu_group.index} seperatly')
                    new_group_list = bbu_group.seperate()
                    if new_group_list:
                        for new_group in new_group_list:
                            print(f'assigning pci for new group: {new_group.index}')
                            planned_bbu_result = new_group.pick_pci(try_3_tier=try_3_tier, restrict_sss=restrict_sss,
                                                                    sort_type=plan_type,
                                                                    must_equal_zero=must_equal_zero)
                            if planned_bbu_result:
                                plan_result.update(planned_bbu_result)
                            else:
                                print(f'Can not assign pci for group: {new_group.index} perfectly')
                                break
                    else:
                        print(f'{bbu_group.index} seperate failed.')
                else:
                    print(f'Can not assign pci for group: {bbu_group_index} perfectly')
                    break

        t3 = time.time()
        if assign_3_tier:
            print(f'{i} + {j} of {num_of_bbu_group} {int(t3 - t2)}s')
        else:
            print(f'{i} of {num_of_bbu_group} {int(t3 - t2)}s')
    pci_conflict_list = get_2way(city, arfcn_list).values()
    print(f'length of pci_conflict_list: {len(pci_conflict_list)}')

    t5 = time.time()
    print(f'Planning {len(plan_result)} cells in {int(t5 - t1)} seconds:')


def get_data_ready(table_name):
    mydb = db_connector.PostgresDb()
    mydb.prepare_data(table_name)


def export_cells(export_file=result_file, export_cities=[], arfcn_list=[], ):
    with open(export_file, 'w') as f:
        csv_write = csv.writer(f)
        csv_head = ['enodeb_id', 'cell_id', 'pci', 'can_be_changed']  # , 'plan_in_tiers', 'group_index']
        csv_write.writerow(csv_head)
        for cell in cells.values():
            if ((len(export_cities) == 0 or cell.city in export_cities)
                    and (len(arfcn_list) == 0 or cell.arfcn in arfcn_list)):
                csv_write.writerow([cell.enodeb_id, cell.cell_id, cell.pci, cell.pci_changed])
                # , cell.pci_plan_in_tiers, cell.parent_group.index])
    print('export results done!')


def import_cells(checking_cities=[], arfcn_list=[]):
    """
    1、导入小区及相邻关系数据；
    2、将BBU的小区按覆盖位置分组
    3、将需要规划的小区放入cells_to_plan列表
    :param checking_cities: 指定地市
    :param arfcn_list:      指定频点
    :return:
    """
    # 导入小区及相邻关系数据
    t1 = time.time()
    if not get_cells(cities=checking_cities, auto_neighbor=True):
        print(f'failed in getting cells.')

    # 读取两种关系数据，确保不遗漏
    get_relations(cities=checking_cities, relation_type=Data_Type_Coverage, auto_neighbor=True)
    get_relations(cities=checking_cities, relation_type=Data_Type_HO, auto_neighbor=True)

    t2 = time.time()
    print(f'Import completed in {int(t2 - t1)}s.')
    # 将BBU的小区按覆盖位置分组
    print(f'there are {len(enodebs)} eNodeBs.')
    for bbu in enodebs.values():
        bbu.detect_same_location(arfcn_list=arfcn_list)

    # 确认需要规划的小区清单
    # global cells_to_plan_list
    for city in checking_cities:
        # 先将涉及到的小区放入cells_to_plan列表
        for cell in cells.values():
            if (len(arfcn_list) == 0 or cell.arfcn in arfcn_list) and cell.city == city:
                cells_to_plan_list.append(cell)

                if cell.parent_group not in groups_to_plan_list:
                    groups_to_plan_list.append(cell.parent_group)
        print(f'There are {len(cells_to_plan_list)} cells in {arfcn_list} need to be planned.')
        print(f'There are {len(groups_to_plan_list)} cell_groups need to be planned.')


def import_data_from_excel_(in_file, to_table, clear_before_insert=False):
    mydb = db_connector.PostgresDb()
    cur=mydb.conn.cursor()
    if not mydb.table_is_exist(to_table):
        mydb.create_coverage_table(to_table)
    else:
        if clear_before_insert:
            print('Deleting existing data')
            cur.execute(f'delete from {to_table};')
            cur.execute('commit;')
        wb = xlrd.open_workbook(filename=in_file)  # 打开文件
        sheet1 = wb.sheet_by_index(0)
        num_of_rows = sheet1.nrows
        print(f'Total {num_of_rows} rows')
        succeeded = 0
        sql_string = ''
        for i in range(1, num_of_rows):
            content = sheet1.row_values(i)
            return_sql_stirng = mydb.insert_data(in_data=content, table_name=to_table)
            if return_sql_stirng:
                succeeded += 1
                sql_string += return_sql_stirng
        print(f'Inserting {succeeded} records')
        # print(sql_string)
        cur.execute(sql_string)
        cur.execute('commit;')
        print('Done!')


if __name__ == '__main__':
    mydb = db_connector.PostgresDb()
    tbl_name_coverage = 'cell_coverage_gd_20210206'
    tbl_name_relation = 'cell_relation_gd_20210206'
    geom_index = f'geom_idex_{tbl_name_coverage}'
    checking_cities = ['深圳', '汕头', '潮州', '揭阳', '汕尾', '广州', '中山', '清远', '韶关',
                       '东莞', '惠州', '河源', '梅州', '佛山', '阳江', '茂名', '湛江', '江门', '珠海', '肇庆', '云浮']
    # checking_cities =  ['GZ', 'DG', 'FS', 'HY', 'HZ', 'JM', 'JY', 'MM', 'MZ', 'QY', 'SG',
    #                     'ST', 'SW', 'SZ', 'YF', 'YJ', 'ZH', 'ZJ', 'ZQ', 'CZ', 'ZS']
    data_files = ['2月覆盖多边形12片区.xlsx', '2月覆盖多边形345片区.xlsx']
    data_path = '/Users/nathantse/Downloads/00 temp/'

    if not mydb.table_is_exist(tbl_name_coverage):
        mydb.create_coverage_table(table_name=tbl_name_coverage, geom_idx_name=geom_index)
    clear_before_insert = True
    for data_file in data_files:
        in_file = f'{data_path}{data_file}'
        import_data_from_excel_(in_file=in_file, to_table=tbl_name_coverage, clear_before_insert=clear_before_insert)
        #  只有第一个导入时才清空
        if clear_before_insert:
            clear_before_insert = False
    mydb.prepare_data(tbl_name_coverage, geom_idx=geom_index, handle_polygon=True, create_index=True, calc_area=True)
    mydb.generate_relation(tbl_name_coverage, tbl_name_relation, checking_cities)

    # arfcn_list = lte_coverage.band3
    #
    # import_cells(checking_cities=checking_cities, arfcn_list=arfcn_list)
    # plan_pci2(checking_cities=checking_cities, arfcn_list=arfcn_list, reset_pci=True, top_big_cells=True,
    #           restrict_sss=set(range(140, 168)), plan_type=0, assign_3_tier=False, must_equal_zero=True)
    # export_cells(export_file=result_file, export_cities=checking_cities, arfcn_list=arfcn_list)
