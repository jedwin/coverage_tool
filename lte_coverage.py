from geopy.distance import geodesic
import math
import time
import pandas as pd
import logging

MAX_PCI_LTE = 503  # PCI最大值
MAX_SSS = 167  # sss最大值
MAX_RANK = 9999  # 预留PCI各个RANK的初始值，应足够大以避免误使用
isolate_cell_thresh = 0  # 孤岛小区的判断门限，重叠小区数量小于isolate_cell_thresh * level_depth则为孤岛，为0代表忽略
# 为了PCI重叠面积分数和模3重叠面积分数可比，需要乘以此扩大系数。
# 由于1个小区计算三层重叠后通常会和200~350个pci有交集，因此暂定次系数为250
scale_factor = 1
mod3_factor = 1  # 规划时模3影响系数，设置越大则越要避免模3
overlap_factor = 10  # 规划时重叠面积的影响系数，设置越大则越要求更小的综合重叠面积
reuse_factor = 0  # 规划时地市内复用次数的影响系数，设置越大要求更小的复用次数，PCI使用更平均
distance_factor = 0  # 规划时同PCI小区距离的影响系数，设置越大则要求更远的间隔距离

Optimize_FACTOR = 1  # 新PCI的rank必须小于原PCI rank乘以这个系数才会选取
level_factor = [1, 1, 0.8, 0.5, 0.3]  # 每层重叠面积的系数，暂定5层
level_factor_default = 0.01  # 超过5层外，默认使用的系数
level_depth = 2  # 计算邻区重叠层数，默认2层，3层时速度可以接受。4层时速度降低大约60倍
same_location_thresh = 10  # 同BBU小区之间如果距离小于此门限，就判断为共址小区

cells = {}  # 整体小区集合{index: cell}
enodebs = {}  # 整体基站集合{index: enodeb}
cell_size_dict = {}
enodeb_group_size_dict = {}
cell_size_ladder = []  # 小区覆盖范围天梯榜，[(cell_index, number_of_all_tier_cells)]
enodeb_group_size_ladder = []  # 基站覆盖范围天梯榜，[(enodeb_id, number_of_all_tier_enodebs)]
series_enodeb_group_size = pd.Series()  # 用于存储每个共址小区组的1,2层相邻小区数，排序依据之一
series_enodeb_group_ava_sss = pd.Series()  # 用于存储每个共址小区组的可用sss个数，排序依据之一
df_enodeb_group_priority = pd.DataFrame()  # 用于存放每个共址小区组的多个series，然后按策略进行规划次序安排
log_file = 'pci_planning_log.txt'
logging.basicConfig(level=logging.ERROR, format='', filename=log_file, filemode='w')
logger = logging.getLogger('ltecell')
band1 = [75, 100]
band3 = [1825, 1850]


def generate_all_tiers(city='', arfcn_list=None):
    """
    1、为指定city或频点的小区，生成第2、第3层相邻小区属性
    2、
    :param city:
    :param arfcn_list:
    :return:
    """
    if arfcn_list is None:
        arfcn_list = []

    global cell_size_ladder
    global enodeb_group_size_ladder
    global series_enodeb_group_size
    cell_size_ladder = []
    enodeb_group_size_ladder = []
    for cell in cells.values():
        if (len(city) == 0 or cell.city == city) and \
                (len(arfcn_list) == 0 or cell.arfcn in arfcn_list):
            cell.detect_1st_tier_cells()
            cell.detect_2nd_tier_cells()
            cell.detect_3rd_tier_cells()
            cell_size_dict[cell.index] = (len(cell.first_tier_cells) + len(cell.second_tier_cells))
            # + len(cell.third_tier_cells))
            if len(cell.first_tier_cells) > MAX_PCI_LTE:
                print(f'Warning: {cell.index} has {len(cell.first_tier_cells)} neighbors!')
                cell.can_be_changed = False

    for my_enodeb in enodebs.values():
        # 用这个enodeb的第一个小区来判断基站city属性
        cell = list(my_enodeb.cells.values())[0]
        if len(city) == 0 or cell.city == city:
            # 生成基站每个共址小区组的相邻小区清单
            my_enodeb.generate_outer_groups()
            for group in my_enodeb.colocation_cells_groups.values():
                cell = group.cell_list[0]

                if len(arfcn_list) == 0 or cell.arfcn in arfcn_list:
                    enodeb_group_size_dict[f'{group.index}'] = (len(group.first_tier_groups)
                                                                + len(group.second_tier_groups))
                    # + len(group.third_tier_groups))

            series_enodeb_group_size = pd.Series(enodeb_group_size_dict)
    cell_size_ladder = sort_value_in_dict(cell_size_dict, reverse=True)
    print(f'len of cell_size_ladder: {len(cell_size_ladder)}')
    enodeb_size_ladder = sort_value_in_dict(enodeb_group_size_dict, reverse=True)
    print(f'len of enodeb_size_ladder: {len(enodeb_size_ladder)}')

    logger.info(cell_size_ladder)
    logger.info(enodeb_size_ladder)


def sort_value_in_dict(input_dict, reverse=False):
    """
    根据输入的字典value排序，按要求返回排序后的key列表

    :param input_dict: 输入字典，如果为空，则返回空列表[]
    :param reverse: 排序方式，False代表正向，从小到大排，True代表反向排
    :return: 排序后的[key, value]的列表或空列表
    """
    input_length = len(input_dict.items())
    if input_length <= 0:
        return []
    else:
        return [i for i in sorted(input_dict.items(), key=lambda x: x[1], reverse=reverse)]


def check_lat_long(latitude=0, longitude=0):
    """
    检查输入的经纬度是否合法
    :param latitude:
    :param longitude:
    :return: 合法返回True，否则返回False
    """
    if -90 <= latitude <= 90 and -180 <= longitude <= 180:
        return True
    else:
        return False


def update_neighbour_available_pci_set(cell_index, old_pci=-1, new_pci=-1):
    """
    更新相邻小区的available_pci集合属性：
    1、将new_pci从相邻2层小区的可用pci中删除
    2、检查是否可以将old_pci添加回相邻2层小区的可用pci

    :param cell_index: 指示发生了pci改变的小区id，从而更新与其相关的相邻小区available_pci集合，必选参数；
    :param old_pci: 指示cell_index对应小区的旧pci；
    :param new_pci: 指示cell_index对应小区的新pci；
    :return: 更新成功返回True，否则返回False
    """
    if len(cell_index) > 0:
        if cell_index in cells.keys():
            pci_changed_cell = cells[cell_index]
            # 更新第一层相邻小区
            for first_tier_cell in pci_changed_cell.first_tier_cells.values():
                first_tier_cell.available_pci.discard(new_pci)
                first_tier_cell.available_pci_3_tiers.discard(new_pci)
                if old_pci > -1:
                    used_pci = set()
                    # 遍历first_tier_cell的第一层邻区，将pci加入到used_pci集合中
                    for ncell in first_tier_cell.first_tier_cells.values():
                        used_pci.add(ncell.pci)
                    # 遍历first_tier_cell的第二层邻区，将pci加入到used_pci集合中
                    for nncell in first_tier_cell.second_tier_cells.values():
                        used_pci.add(nncell.pci)
                    if old_pci not in used_pci:
                        first_tier_cell.available_pci.add(old_pci)
                first_tier_cell.calculate_available_sss()
            # 更新第二层相邻小区
            for second_tier_cell in pci_changed_cell.second_tier_cells.values():
                second_tier_cell.available_pci.discard(new_pci)
                second_tier_cell.available_pci_3_tiers.discard(new_pci)
                if old_pci > -1:
                    used_pci = set()
                    # 遍历second_tier_cell的第一层邻区，将pci加入到used_pci集合中
                    for ncell in second_tier_cell.first_tier_cells.values():
                        used_pci.add(ncell.pci)
                    # 遍历first_tier_cell的第二层邻区，将pci加入到used_pci集合中
                    for nncell in second_tier_cell.second_tier_cells.values():
                        used_pci.add(nncell.pci)
                    if old_pci not in used_pci:
                        second_tier_cell.available_pci.add(old_pci)
                second_tier_cell.calculate_available_sss()
            # for third_tier_cell in pci_changed_cell.third_tier_cells.values():
            #     third_tier_cell.available_pci_3_tiers.discard(new_pci)
            #     third_tier_cell.calculate_available_sss()
            # 第三层小区暂时不对old_pci进行检查
            return True
        else:
            logger.error(f'invalid cell_index: {cell_index}')
            return False
    else:
        logger.error(f'cell_index is blank')
        return False


def upate_neighbor_group_available_sss(enodeb_cell_group):
    for current_group in enodeb_cell_group.first_tier_groups:
        current_group.generate_available_sss()
    for current_group in enodeb_cell_group.second_tier_groups:
        current_group.generate_available_sss()


def try_assign_sss_to_group(sss_list, cell_list):
    """
    尝试按给定的sss顺序给一个小区组内的最多3个小区分配PCI，仅对小区的available_pci集合进行验证
    :param sss_list: 用于分配的sss列表，按顺序进行尝试
    :param cell_list: 一个小区列表，内含最多3个小区
    :return: 如果该sss在指定层数内不混淆，返回分配结果字典{cell_index：new_pci}，否则返回空字典{}
    """
    return_dict = {}
    cells_to_evaluate = len(cell_list)

    all_cells_pci_sets = []  # 使用基于顺序的嵌套列表 [[cell0_index, cell0_pci_set], [cell1_index, cell1_pci_set], ...]
    found_no_overlap_pci = False
    cell_index_list = []
    t_assign_sss_to_group_start = time.time()
    try_count = 0
    for sss in sss_list:
        try_count += 1
        new_pci_set = {sss * 3, sss * 3 + 1, sss * 3 + 2}
        if cells_to_evaluate == 1:
            cell0_pci_set = new_pci_set.intersection(cell_list[0].available_pci)
            if cell0_pci_set:
                found_no_overlap_pci = True
                all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])

        elif cells_to_evaluate == 2:
            cell0_pci_set = new_pci_set.intersection(cell_list[0].available_pci)
            cell1_pci_set = new_pci_set.intersection(cell_list[1].available_pci)
            combined_pci_set01 = cell0_pci_set.union(cell1_pci_set)
            if cell0_pci_set and cell1_pci_set and len(combined_pci_set01) >= 2:
                found_no_overlap_pci = True
                # 顺序很重要，如果存在只有1个可用pci的，必须排前面
                if len(cell0_pci_set) == 1:
                    all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                    all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                else:
                    all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                    all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])

        elif cells_to_evaluate == 3:
            cell0_pci_set = new_pci_set.intersection(cell_list[0].available_pci)
            cell1_pci_set = new_pci_set.intersection(cell_list[1].available_pci)
            cell2_pci_set = new_pci_set.intersection(cell_list[2].available_pci)
            if cell0_pci_set and cell1_pci_set and cell2_pci_set:
                combined_pci_set01 = cell0_pci_set.union(cell1_pci_set)
                combined_pci_set02 = cell0_pci_set.union(cell2_pci_set)
                combined_pci_set12 = cell1_pci_set.union(cell2_pci_set)
                combined_pci_set012 = combined_pci_set01.union(cell2_pci_set)
                if (len(combined_pci_set01) >= 2 and len(combined_pci_set02) >= 2 and
                        len(combined_pci_set12) >= 2 and len(combined_pci_set012) >= 3):
                    found_no_overlap_pci = True
                    # 顺序很重要，如果某两个小区的并集是2，则这两个小区必须排前面；如果存在只有1个可用pci的，必须排前面；
                    if len(combined_pci_set01) == 2:
                        if len(cell0_pci_set) == 1:
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                    elif len(combined_pci_set02) == 2:
                        if len(cell0_pci_set) == 1:
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                    elif len(combined_pci_set12) == 2:
                        if len(cell1_pci_set) == 1:
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                    elif len(cell0_pci_set) == 2 and len(cell1_pci_set) == 2 and len(cell2_pci_set) == 2:
                        # 如果三个set里面都只有2个pci，而两两并集又都有3个，则需要额外处理
                        # 需要对每个set里面的pci进行排序，防止出现类似{A，B}、{C，B}和{A，C}的情况
                        pci0_in_set0 = list(cell0_pci_set)[0]
                        pci0_in_set1 = list(cell1_pci_set)[0]
                        if pci0_in_set0 in cell2_pci_set and pci0_in_set1 in cell2_pci_set:
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                    else:
                        all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                        all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                        all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                        all_cells_pci_sets = sorted(all_cells_pci_sets, key=lambda x: len(x[1]))
        if found_no_overlap_pci:
            break
    t_assign_sss_to_group_mid = time.time()
    logger.debug(f'found sss for {try_count} times in '
                 f'{int((t_assign_sss_to_group_mid - t_assign_sss_to_group_start) * 1000)}ms')
    if found_no_overlap_pci:
        # print(f'all_cells_pci_sets: {all_cells_pci_sets}')
        for i in range(0, cells_to_evaluate):
            cell_index, cell_pci_set = all_cells_pci_sets.pop(0)
            new_pci = list(cell_pci_set)[0]
            current_cell = cells[cell_index]
            t1 = time.time()
            current_cell.set_pci(new_pci)
            logger.debug(f'setting pci in {int((time.time() - t1) * 1000)}ms')
            current_cell.pci_plan_in_tiers = 2
            for cell_index, cell_pci_set in all_cells_pci_sets:
                cell_pci_set.discard(new_pci)
            return_dict[cell_index] = new_pci
        current_cell.parent_group.generate_available_sss()
        current_cell.parent_group.is_done = True
        upate_neighbor_group_available_sss(current_cell.parent_group)
    t_assign_sss_to_group_end = time.time()
    logger.debug(f'assign_sss_to_group in '
                 f'{int((t_assign_sss_to_group_end - t_assign_sss_to_group_mid) * 1000)}ms')
    logger.debug(f'assign_result: {return_dict}')
    return return_dict


def try_assign_sss_to_group_3_tiers(sss_list, cell_list):
    """
    尝试按给定的sss顺序给一个小区组内的最多3个小区分配PCI，本函数仅对小区的available_pci_3_tiers集合进行验证。
    :param sss_list: 用于分配的sss列表，按顺序进行尝试
    :param cell_list: 一个小区列表，内含最多3个小区
    :return: 如果该sss在指定层数内不混淆，返回分配结果字典{cell_index：new_pci}，否则返回空字典{}
    """
    return_dict = {}
    cells_to_evaluate = len(cell_list)

    all_cells_pci_sets = []  # 使用基于顺序的嵌套列表 [[cell0_index, cell0_pci_set], [cell1_index, cell1_pci_set], ...]
    found_no_overlap_pci = False
    for sss in sss_list:
        new_pci_set = {sss * 3, sss * 3 + 1, sss * 3 + 2}
        if cells_to_evaluate == 1:
            cell0_pci_set = new_pci_set.intersection(cell_list[0].available_pci_3_tiers)
            if cell0_pci_set:
                found_no_overlap_pci = True
                all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])

        elif cells_to_evaluate == 2:
            cell0_pci_set = new_pci_set.intersection(cell_list[0].available_pci_3_tiers)
            cell1_pci_set = new_pci_set.intersection(cell_list[1].available_pci_3_tiers)
            combined_pci_set01 = cell0_pci_set.union(cell1_pci_set)
            if cell0_pci_set and cell1_pci_set and len(combined_pci_set01) >= 2:
                found_no_overlap_pci = True
                # 顺序很重要，如果存在只有1个可用pci的，必须排前面
                if len(cell0_pci_set) == 1:
                    all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                    all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                else:
                    all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                    all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])

        elif cells_to_evaluate == 3:
            cell0_pci_set = new_pci_set.intersection(cell_list[0].available_pci_3_tiers)
            cell1_pci_set = new_pci_set.intersection(cell_list[1].available_pci_3_tiers)
            cell2_pci_set = new_pci_set.intersection(cell_list[2].available_pci_3_tiers)
            if cell0_pci_set and cell1_pci_set and cell2_pci_set:
                combined_pci_set01 = cell0_pci_set.union(cell1_pci_set)
                combined_pci_set02 = cell0_pci_set.union(cell2_pci_set)
                combined_pci_set12 = cell1_pci_set.union(cell2_pci_set)
                combined_pci_set012 = combined_pci_set01.union(cell2_pci_set)
                if (len(combined_pci_set01) >= 2 and len(combined_pci_set02) >= 2 and
                        len(combined_pci_set12) >= 2 and len(combined_pci_set012) >= 3):
                    found_no_overlap_pci = True
                    # 顺序很重要，如果某两个小区的并集是2，则这两个小区必须排前面；如果存在只有1个可用pci的，必须排前面；
                    if len(combined_pci_set01) == 2:
                        if len(cell0_pci_set) == 1:
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                    elif len(combined_pci_set02) == 2:
                        if len(cell0_pci_set) == 1:
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                    elif len(combined_pci_set12) == 2:
                        if len(cell1_pci_set) == 1:
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                    elif len(cell0_pci_set) == 2 and len(cell1_pci_set) == 2 and len(cell2_pci_set) == 2:
                        # 如果三个set里面都只有2个pci，而两两并集又都有3个，则需要额外处理
                        # 需要对每个set里面的pci进行排序，防止出现类似{A，B}、{C，B}和{A，C}的情况
                        pci0_in_set0 = list(cell0_pci_set)[0]
                        pci0_in_set1 = list(cell1_pci_set)[0]
                        if pci0_in_set0 in cell2_pci_set and pci0_in_set1 in cell2_pci_set:
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                        else:
                            all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                            all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                            all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                    else:
                        all_cells_pci_sets.append([cell_list[0].index, cell0_pci_set])
                        all_cells_pci_sets.append([cell_list[1].index, cell1_pci_set])
                        all_cells_pci_sets.append([cell_list[2].index, cell2_pci_set])
                        all_cells_pci_sets = sorted(all_cells_pci_sets, key=lambda x: len(x[1]))
        if found_no_overlap_pci:
            break

    if found_no_overlap_pci:
        # print(f'all_cells_pci_sets: {all_cells_pci_sets}')
        for i in range(0, cells_to_evaluate):
            cell_index, cell_pci_set = all_cells_pci_sets.pop(0)
            new_pci = list(cell_pci_set)[0]
            current_cell = cells[cell_index]
            logger.debug(f'cell_index: {cell_index}, current cells_pci_set: {cell_pci_set}, new_pci: {new_pci}')
            current_cell.set_pci(new_pci)
            current_cell.pci_plan_in_tiers = 3
            for cell_index, cell_pci_set in all_cells_pci_sets:
                cell_pci_set.discard(new_pci)
            return_dict[cell_index] = new_pci
        current_cell.parent_group.generate_available_sss()
        current_cell.parent_group.is_done = True
        upate_neighbor_group_available_sss(current_cell.parent_group)
    return return_dict


class enodeb:
    def __init__(self, **kwargs):
        self.enodeb_cell_groups = []  # 将位于3个或以下共址的小区放在一个enodeb_cell_group对象里面，然后多个这样的对象组成这个列表
        self.same_location_groups = []  # 将位于相近位置的小区放在一个list里面，然后多个这样的list组成same_location_groups
        self.city = kwargs['city']
        self.enodeb_id = kwargs['enodeb_id']
        self.cells = {}  # 本bbu内所包含的小区{cell_index: cell}
        self.colocation_cells_groups = {}  # 将本bbu内的需要规划PCI的小区按相邻地理位置分组，用于pci分组规划。
        # {group_id: enodeb_cell_group对象}
        self.sss_rank = {}  # 对于每个sss的总体评分{sss_offset: rank}
        self.sss_overlap_rank = {}  # 对于每个sss的重叠评分{sss_offset: overlap_rank}
        self.first_tier_cells = {}  # 和本enodeb有任何小区相邻的cell，按colocation_cells_groups分组统计。
        # 示例{group_id: [cell list]}
        self.second_tier_cells = {}  # 和本enodeb的任何小区都相隔1层的cell，按colocation_cells_groups分组统计
        # 示例{group_id: [cell list]}
        self.third_tier_cells = {}  # 和本enodeb的任何小区都相隔2层的cell，按colocation_cells_groups分组统计
        # 示例{group_id: [cell list]}
        self.colocation_group_available_sss = {}  # 为每个colocation_cells_group id计算可用的sss个数
        # 示例：{group_id: [
        self.group_preferred_sss = {}  # 存在每个频点优选的sss，用于规划pci。避开adjacent和second_tier，
        # 然后优先选择third_tier 用过的sss {group_id：[sss list]}
        self.max_group_number = 0  # bbu内分组后的最大组号

    def detect_same_location(self, arfcn_list=None, group_type=2):
        """
        方案1: 基于小区重叠：将bbu内所有小区按位置分组，小区直接重叠（或有切换关系）或者经纬度距离小于门限则为同一个
        same location组
        方案2：基于小区原PCI：将同band，同sss的小区归为同一个group


        :return: 填充colocation_cells_groups，内有一个或多个colocation_cells_group，组内包括同多个小区
        [[cell1, cell2], [cell3, cell4, cell5]]
        """
        if arfcn_list is None:
            arfcn_list = []
        i = 0  # 组号，自动递增
        for current_cell in self.cells.values():
            if (len(arfcn_list) == 0 or current_cell.arfcn in arfcn_list) and current_cell.can_be_changed \
                    and current_cell.city:
                cell_in_group = False
                for group_id, group in self.colocation_cells_groups.items():
                    if len(group.cell_list) >= 3:
                        # 如果当前group已满3个小区，则继续下一个group
                        continue
                    for cell in group.cell_list:
                        if group_type == 1:
                            if current_cell.index in cell.adjacent_cells.keys() or \
                                    cell.distance(current_cell) < same_location_thresh:
                                # current_cell和当前group内的当前cell有邻接关系，可以归入当前group，跳出组内循环到下一个group
                                group.cell_list.append(current_cell)
                                current_cell.parent_group_id = group_id
                                current_cell.parent_group = group
                                cell_in_group = True
                                break
                        else:  # 按原PCI分组，使用同一个sss的归入一组
                            if current_cell.band == cell.band and current_cell.org_sss == cell.org_sss:
                                # current_cell和当前group内的当前cell使用相同的sss，可以归入当前group，跳出组内循环到下一个group
                                group.cell_list.append(current_cell)
                                current_cell.parent_group_id = group_id
                                current_cell.parent_group = group
                                cell_in_group = True
                                break
                    if cell_in_group:
                        # 如果已经为current_cell分配组，则跳出组间循环，到下一个未判断小区
                        # 否则继续下一个group
                        break
                if not cell_in_group:
                    # 新建一个enodeb_cell_group对象
                    new_group = enodeb_cells_group(enodeb_id=self.enodeb_id, group_id=i, cell_list=[current_cell])
                    self.colocation_cells_groups[i] = new_group
                    self.max_group_number = i
                    current_cell.parent_group_id = i
                    current_cell.parent_group = new_group
                    i += 1

    def generate_outer_groups(self):
        """
        生成基站共址小区组的前3层相邻组：

        :return:
        """
        for current_group in self.colocation_cells_groups.values():
            # 计算该组的第1层相邻组：组内每个小区的第一层小区所属的组的去重列表
            for cell in current_group.cell_list:
                for current_cell_index in cell.first_tier_cells:
                    current_cell = cells[current_cell_index]
                    if current_cell.parent_group not in current_group.first_tier_groups:
                        current_group.first_tier_groups.append(current_cell.parent_group)
            # 计算该组的第2层相邻组：组内每个小区的第2层小区所属的组的去重列表，再剔除已在第1层中的组
            for cell in current_group.cell_list:
                for current_cell_index in cell.second_tier_cells:
                    current_cell = cells[current_cell_index]
                    if (current_cell.parent_group not in current_group.second_tier_groups and
                            current_cell.parent_group not in current_group.first_tier_groups):
                        current_group.second_tier_groups.append(current_cell.parent_group)
            # 计算该组的第3层相邻组：组内每个小区的第3层小区所属的组的去重列表，再剔除已在第1,2层中的组
            for cell in current_group.cell_list:
                for current_cell_index in cell.third_tier_cells:
                    current_cell = cells[current_cell_index]
                    if (current_cell.parent_group not in current_group.third_tier_groups and
                            current_cell.parent_group not in current_group.second_tier_groups and
                            current_cell.parent_group not in current_group.first_tier_groups):
                        current_group.second_tier_groups.append(current_cell.parent_group)

    def get_preferred_sss(self, group_id=-1, sort_type=0, restrict_sss=[]):
        """
        为指定小区组生成优选sss列表
        :param restrict_sss:
        :param sort_type: 指定排序规则
        :param group_id: enodeb的小区组编号
        :return: 返回优选sss列表，失败时返回空列表
        """
        if group_id < 0:
            return []
        else:
            return_list = self.colocation_cells_groups[group_id].get_preferred_sss(sort_type=sort_type,
                                                                                   restrict_sss=restrict_sss)
            return return_list

    def pick_pci(self, restrict_group=False, restrict_sss=None, sort_type=3,
                 must_equal_zero=True, try_3_tier=True,
                 colocation_cells_group_id=-1):
        """
        1.1 以BBU为单位进行PCI规划，首先将BBU内所有指定频点（arfcn参数）的小区放进planning_cells{}
        1.2 将planning_cells按cell_id排序，放入sorted_cells[]
        2、当强制要求按3个PCI一组进行分配时，
            2.1 计算planning_cells个数，放入number_of_cells
            2.2 将planning_cells里的小区按3个为1组进行分组, 组数为cell_groups（从0开始）
            2.3 同时将pci也按3个分组，将1组小区对1组pci，进行轮换统计，这组小区对每组pci的分数放入sss_rank{}
            2.4 如果组里面有任意1个PCI不能使用（预留），则该组不参与评分
            2.5 最后选sss_rank{}中排名最前的pci组和轮换偏置作为这组小区的规划结果
        3、当不要求按组分配时，直接对所有planning_cells遍历执行pick_pci()和set_pci()

        :param sort_type:
        :type restrict_sss:
        :param try_3_tier: 是否尝试进行3层邻区，True会导致使用更多PCI，但隔离度更好；False则可节省更多PCI
        :param restrict_sss: 保留的sss集合，不用于分配
        :param colocation_cells_group_id: 指定对某个共用pci组的1~3个小区进行规划，如果为-1表示全部组
        :param must_equal_zero: 是否要求结果必须找到无混淆的PCI，如果为True时，遇到无法分配就直接退出，否则将会选一个分数最优的结果
        :param restrict_group: 指定是否强制要求按3个PCI一组进行分配，默认不强制。
        :return: 规划成功返回return_dict={cell_index: new_pci}，否则返回空{}
        """
        planning_cells = {}
        planning_groups = {}
        if colocation_cells_group_id == -1:
            # for cell_index, cell in self.cells.items():
            #     # print(cell_index, cell.arfcn)
            #     if (len(arfcn_list) or cell.arfcn in arfcn_list) and cell.can_be_changed:
            #         planning_cells[cell_index] = cell
            planning_groups = self.colocation_cells_groups
        else:
            planning_groups[colocation_cells_group_id] = self.colocation_cells_groups[colocation_cells_group_id]
        # number_of_cells = len(planning_cells)
        # if number_of_cells == 0:
        #     return {}

        # 先将bbu内所有需要规划pci的小区按cellid排序，返回小区list
        # 然后将需要规划pci的小区按是否互相重叠来分大组，最后再在大组内按小区编号3个分一组
        # x[0]为cell_index，即enodebid_cellid，因此按'_'分列后去第2个就是cellid
        # 返回的item为（cell_index, cell对象)，所以要用item[1]

        offsets = {0: [0, 1, 2],
                   1: [0, 2, 1],
                   2: [1, 0, 2],
                   3: [1, 2, 0],
                   4: [2, 0, 1],
                   5: [2, 1, 0]}
        return_dict = {}
        if restrict_group:
            t_pick_pci_start = time.time()
            for group_id, group in planning_groups.items():
                current_group = f'{self.enodeb_id}_{group_id}'
                logger.info(f'{current_group} size: {enodeb_group_size_dict[current_group]}')
                number_of_cells = len(group.cell_list)
                cells_to_evaluate = number_of_cells

                # 通过集合方式剔除保留PCI对应的sss
                sss_candidate_list = self.get_preferred_sss(group_id=group_id,
                                                            sort_type=sort_type, restrict_sss=list(restrict_sss))
                # logger.debug(f'current group:{current_group}, sss_candidate_list:{sss_candidate_list}')
                # 开始规划
                if try_3_tier:
                    return_dict = try_assign_sss_to_group_3_tiers(sss_list=sss_candidate_list
                                                                  , cell_list=group.cell_list)
                    if len(return_dict) > 0:
                        break

                return_dict = try_assign_sss_to_group(sss_list=sss_candidate_list, cell_list=group.cell_list)
                if len(return_dict) > 0:
                    break
                else:
                    # print(f'Can not assign pci for group: {self.enodeb_id}_{colocation_cells_group_id} perfectly')
                    if must_equal_zero:
                        return {}
                    # best_offset_for_sss, best_sss_rank = sort_value_in_dict(self.sss_rank)[0]
                    # best_sss, best_offset_index = best_offset_for_sss.split('_')
                    # for j in range(0, cells_to_evaluate):
                    #     new_pci = int(best_sss) * 3 + offsets[int(best_offset_index)][j]
                    #     current_cell = group[j]
                    #     if current_cell.set_pci(new_pci):
                    #         return_dict[current_cell.index] = new_pci
                    #         if best_sss in self.colocation_group_available_sss[group_id]:
                    #             self.colocation_group_available_sss[group_id].remove(best_sss)

        else:  # 无需按3个pci一组来分配
            for current_cell in planning_cells.values():
                new_pci = current_cell.pick_pci()
                if new_pci:  # 如果pick_pci失败，则返回False
                    current_cell.set_pci(new_pci)
                    return_dict[current_cell.index] = new_pci
                else:
                    print('pick pci failed')
                    return {}
        t_pick_pci_end = time.time()
        return return_dict


class LteCell:
    city_arfcn_pci_count = {}  # 类变量，存放地市级别数据，内含三层字典：
    # {city1: {arfcn1: {pci1: count, pci2: count}, arfcn2: {...}},
    # city2: {arfcn1: {pci1: count, pci2: count}, arfcn2: {...}}
    # ...}}
    city_arfcn_sss_count = {}  # 类变量，统计

    def __init__(self, **kwargs):
        self.city = kwargs['city']

        self.enodeb_id = kwargs['enodeb_id']
        self.cell_id = kwargs['cell_id']
        self.arfcn = kwargs['arfcn']
        if self.arfcn in range(600):
            self.band = 1
        elif self.arfcn in range(1200, 1950):
            self.band = 3
        elif self.arfcn in range(2400, 2650):
            self.band = 5
        elif self.arfcn in range(8690, 9040):
            self.band = 26
        elif self.arfcn in range(39650, 41590):
            self.band = 41
        else:
            self.band = 0
        self.area = kwargs['area']
        self.pci = kwargs['pci']
        self.sss = math.floor(self.pci / 3)
        self.org_pci = kwargs['pci']
        self.org_sss = self.sss
        self.can_be_changed = kwargs['can_be_changed']  # 指示能否对其规划PCI
        self.pci_changed = False
        self.index = '%d_%d' % (int(self.enodeb_id), int(self.cell_id))
        self.parent = None
        self.parent_group = None  # 该小区所属的bbu共址组
        self.parent_group_id = -1  # 该小区所属的bbu共址组id
        self.adjacent_cells = {}  # 第1层小区，包括异频{nCell.index: [nCell, overlaps_area]}
        self.first_tier_cells = {}  # 第1层同频小区{nCell1.index: nCell1, nCell2.index: nCell2, ...}
        self.second_tier_cells = {}  # 第2层同频小区{nnCell1.index: nnCell1, nnCell2.index: nnCell2, ...}
        self.third_tier_cells = {}  # 第3层同频小区{nnnCell1.index: nnnCell1, nnnCell2.index: nnnCell2, ...}
        self.pci_plan_in_tiers = 1  # 标记使用了2还是3层同频小区组进行规划，1表示不涉及, 0表示为单小区规划
        self.adjacent_arfcn_pci = {}
        # 储存当前本小区可用的PCI集合，每次某小区set_pci时将更新其相邻周围2层小区的该属性，初始化为所有PCI
        self.available_pci = set(list(i for i in range(MAX_PCI_LTE + 1)))
        self.available_sss = set(range(MAX_SSS + 1))
        # 考虑3层同频相邻小区时的可用PCI集合，初始化为所有PCI
        self.available_pci_3_tiers = set(list(i for i in range(MAX_PCI_LTE + 1)))
        self.available_sss_3_tiers = set(range(MAX_SSS + 1))
        self.__pci_overlap_rank_temp = {}
        self.pci_overlap_rank = {}
        self.pci_mod3_rank = {}
        self.pci_reuse_rank = {}
        self.pci_distance_rank = {}
        self.pci_rank = {}
        self.df_pci_rank = pd.DataFrame()
        self.latitude = 0
        self.longitude = 0
        self.total_overlaps_area = 0
        self.related_cells = 0  # 计算pci_overlap_ranks时统计n层内共有多少小区，用于判断孤立小区

    def add_adj_cell(self, n_cell, overlaps_area):
        self.adjacent_cells[n_cell.index] = [n_cell, overlaps_area]

    def calculate_available_sss(self):
        self.available_sss = set([math.floor(i / 3) for i in self.available_pci])
        self.available_sss_3_tiers = set([math.floor(i / 3) for i in self.available_pci_3_tiers])

    def detect_1st_tier_cells(self):
        self.first_tier_cells = {}
        for adj_cell, _ in self.adjacent_cells.values():
            # 在添加第一层相邻同频小区时，顺便把对应的PCI从可用PCI集合中删除
            if adj_cell.arfcn == self.arfcn:
                self.first_tier_cells[adj_cell.index] = adj_cell
                self.available_pci.discard(adj_cell.pci)
                self.available_pci_3_tiers.discard(adj_cell.pci)

    def detect_2nd_tier_cells(self):
        self.second_tier_cells = {}
        temp_list = []
        for adj_cell, _ in self.adjacent_cells.values():
            temp_list += adj_cell.adjacent_cells.keys()
        temp_set = set(temp_list)
        adjacent_set = set(self.adjacent_cells.keys())
        temp_set -= adjacent_set
        temp_set -= set(self.index)
        # 在统计第二层相邻小区时，顺便把对应的PCI从可用PCI集合中删除
        for cell_index in temp_set:
            ncell = cells[cell_index]
            if ncell.arfcn == self.arfcn:
                self.available_pci.discard(ncell.pci)
                self.available_pci_3_tiers.discard(ncell.pci)
                self.second_tier_cells[cell_index] = ncell

    def detect_3rd_tier_cells(self):
        self.third_tier_cells = {}
        temp_list = []
        # 直接用第2层同频邻区的所有同频邻区，作为第3层同频邻区集合
        for second_tier_cell in self.second_tier_cells.values():
            temp_list += second_tier_cell.adjacent_cells.keys()
        temp_set = set(temp_list)
        first_tier_set = set(self.first_tier_cells.keys())
        second_tier_set = set(self.second_tier_cells.keys())
        third_tier_set = temp_set - first_tier_set - second_tier_set
        for cell_index in third_tier_set:
            ncell = cells[cell_index]
            if ncell.arfcn == self.arfcn and ncell.index != self.index:
                self.available_pci_3_tiers.discard(ncell.pci)
                self.third_tier_cells[cell_index] = ncell

    def distance(self, cell2):
        """
        计算输入小区与本小区的距离
        如果任一小区经纬度不合规范，返回99999
        :param cell2:
        :return:
        """
        if check_lat_long(latitude=self.latitude, longitude=self.longitude) \
                and check_lat_long(latitude=cell2.latitude, longitude=cell2.longitude):
            location1 = (self.latitude, self.longitude)
            location2 = (cell2.latitude, cell2.longitude)
            return geodesic(location1, location2).m
        else:
            return 99999

    def get_arfcn_pci_dict(self, city='', arfcn_list=None):
        """
        将本小区所有邻接小区按 频点_pci 为key，放入字典中
        :param arfcn_list: 如果指定频点列表，则只统计对应频点的邻区
        :param city: 如果指定city，则只统计归属地市=city的邻区
        :return: 更新自身的adjacent_arfcn_pci属性（字典），内容示例：
                {enodeb_cell_arfcn1_pci1:[enodeb1_cell1, enodeb2_cell2],
                    enodeb_cell_arfcn1_pci2:[enodeb3_cell3, enodeb4_cell4]}
        """
        if arfcn_list is None:
            arfcn_list = []
        self.adjacent_arfcn_pci = {}
        arfcn_pci_index = '%s_%d_%d' % (self.index, self.arfcn, self.pci)
        self.adjacent_arfcn_pci[arfcn_pci_index] = [self.index]
        for nCell, overlaps_area in self.adjacent_cells.values():
            if (len(city) == 0 or nCell.city == city) \
                    and (len(arfcn_list) == 0 or nCell.arfcn in arfcn_list):
                arfcn_pci_index = '%s_%d_%d' % (self.index, nCell.arfcn, nCell.pci)
                if arfcn_pci_index in self.adjacent_arfcn_pci.keys():
                    self.adjacent_arfcn_pci[arfcn_pci_index].append(nCell.index)
                else:
                    self.adjacent_arfcn_pci[arfcn_pci_index] = [nCell.index]

    def gen_pci_overlap_rank(self, curr_level=1, deep_level=2, cell_ol_area_list=None):
        """
        递归函数，通过逐层遍历cell对象的adjacent_cells字典，按pci把对应的重叠面积累计
        :param curr_level: 当前层数
        :param deep_level: 下钻层数
        :param cell_ol_area_list: 每次将当前层所有小区的adjacent_cells.values()传递到下一层
        :return: 不返回，只会对self.pci_overlap_rank{}进行更新  # ，并计算共涉及多少个小区self.related_cells
        """
        if cell_ol_area_list is None:
            cell_ol_area_list = []
        next_level_cell_list = []
        # 在生成前先重置本小区的pci_overlap_rank字典
        if curr_level == 1:
            for pci in range(0, MAX_PCI_LTE + 1):
                self.pci_overlap_rank[pci] = 0
        for cell, overlap_area in cell_ol_area_list:
            if cell.index != self.index:
                next_level_cell_list += cell.adjacent_cells.values()
                if self.arfcn == cell.arfcn:
                    pci = cell.pci
                    if 0 <= pci <= MAX_PCI_LTE:
                        if curr_level < len(level_factor):
                            self.pci_overlap_rank[pci] += overlap_area * level_factor[curr_level]
                        else:
                            self.pci_overlap_rank[pci] += overlap_area * level_factor_default

        if curr_level < deep_level:
            # next_level_cell_list = list(set(next_level_cell_list))
            self.gen_pci_overlap_rank(curr_level + 1, deep_level, next_level_cell_list)

    def gen_pci_rank(self):
        """
        补全所有PCI的评分值，存入self.pci_rank{}
        每个PCI评分= 综合重叠面积分数 * overlap_factor * scale_factor + 综合模3分数 * mod3_factor
                    + 复用次数比例 * reuse_factor + 复用距离分数 * distance_factor
        其中：
        * 综合重叠面积分数 = 如果该PCI在第1层的重叠面积 / 全部PCI的重叠面积 * _1st_tier_FACTOR
        pci_overlap_rank{}  + 如果该PCI在第2层的重叠面积 / 全部PCI的重叠面积 * _2nd_tier_FACTOR
                            + 如果该PCI在第3层的重叠面积 / 全部PCI的重叠面积 * _3rd_tier_FACTOR
        因为有500多个可能性，每个PCI的比例正常应在0~9%左右。为了和模3分数可比，所以要再乘一个扩大系数scale_factor

        * 综合模3分数 = 将所有PCI重叠面积按模3结果 / 全部PCI的重叠面积
        pci_mod3_rank{}   由于只有3种可能，所以其正常范围应为10%~60%左右

        * 复用次数比例 = 每个PCI在该地市同频复用次数 / 该地市同频PCI复用最大次数
        pci_reuse_rank{} 视乎地市PCI复用平均度，其正常范围可能波动比较大，最小为0，最大为1

        * 复用距离 = 未实现
        pci_distance_rank{}

        还没使用的按0计算
        按PCI顺序排好
        第一步：按PCI统计3层同频小区累计重叠面积，即直接接触为1st tier、隔1层为2nd tier、隔2层为3rd tier

        第二步：将上述以PCI为统计维度的结果，按模三相加
        :return: 不返回对象，但会在小区中生成self.pci_rank{}、
        pci_reuse_rank{}、pci_distance_rank{}、pci_overlap_rank{} 和 pci_mod3_rank{}五份字典
        """
        # logger.debug('Generating pci/mod3 rank. Number of adjacent cells: %d' % len(self.adjacent_cells))

        # 先将pci_overlap_rank, pci_distance_rank和pci_rank三个字典初始化
        self.pci_rank = {}
        self.pci_overlap_rank = {}
        for pci in range(0, MAX_PCI_LTE + 1):  # -1用于重置临时值
            self.pci_rank[pci] = 0
            self.pci_overlap_rank[pci] = 0
            self.pci_distance_rank[pci] = 0
        # 初始化模3结果字典
        for key in [0, 1, 2]:
            self.pci_mod3_rank[key] = 0
        # 开始计算pci_overlap_rank{}
        self.gen_pci_overlap_rank(curr_level=1, deep_level=level_depth, cell_ol_area_list=self.adjacent_cells.values())

        # 按模3结果分开统计
        self.total_overlaps_area = 0
        for key, value in self.pci_overlap_rank.items():
            self.pci_mod3_rank[key % 3] += value
            self.total_overlaps_area += value

        if len(self.adjacent_cells) >= isolate_cell_thresh * level_depth:
            self.pci_mod3_rank[0] /= max(self.total_overlaps_area, 1)
            self.pci_mod3_rank[1] /= max(self.total_overlaps_area, 1)
            self.pci_mod3_rank[2] /= max(self.total_overlaps_area, 1)

            tmp_pci_rank_dict = {'overlap_rank': [], 'reuse_rank': [], 'mod3_rank': []}  # 用于构造dataframe格式的字典
            for pci in range(MAX_PCI_LTE + 1):
                self.pci_overlap_rank[pci] /= max(self.total_overlaps_area / scale_factor, 1)
                # 计算pci_reuse_rank，用当前PCI的复用次数/地市同频小区PCI复用最多的次数，应为小于1的浮点数
                self.pci_reuse_rank[pci] = (self.city_arfcn_pci_count[self.city][self.arfcn][pci]
                                            / max(max(self.city_arfcn_pci_count[self.city][self.arfcn].values()), 1))
                self.pci_rank[pci] = (self.pci_overlap_rank[pci] * overlap_factor
                                      + self.pci_mod3_rank[pci % 3] * mod3_factor
                                      + self.pci_reuse_rank[pci] * reuse_factor
                                      + self.pci_distance_rank[pci] * distance_factor)

                tmp_pci_rank_dict['overlap_rank'].append(self.pci_overlap_rank[pci])
                tmp_pci_rank_dict['reuse_rank'].append(self.pci_reuse_rank[pci])
                tmp_pci_rank_dict['mod3_rank'].append(self.pci_mod3_rank[pci % 3])
            self.df_pci_rank = pd.DataFrame(tmp_pci_rank_dict, index=range(MAX_PCI_LTE + 1))

        else:  # 孤岛小区，需要进一步考虑算法实现
            for pci in range(0, MAX_PCI_LTE + 1):
                self.pci_rank[pci] = 0

        logger.debug('self.pci_overlap_rank')
        logger.debug(self.pci_overlap_rank)
        logger.debug('self.pci_reuse_rank')
        logger.debug(self.pci_reuse_rank)
        logger.debug('self.pci_mod3_rank')
        logger.debug(self.pci_mod3_rank)
        logger.debug('sort_value_in_dict(self.pci_rank)')
        logger.debug(sort_value_in_dict(self.pci_rank))

    def pick_pci(self, restrict_pci_list=None):
        """
        为这个小区重新挑选一个PCI，挑选顺序如下：
        按pci_mod3_rank字典中0，1，2值由低到高排序，选出模3结果
        1、剔除相邻1~2层的小区PCI
        剔除限制列表中的PCI
        2、按pci_overlap_rank字典中pci值由低到高排序，选出满足模3要求且分值最低的前N个PCI（排除限制列表中的值）
        3、将上一步中的N个PCI按本地市PCI复用次数排序，选出复用次数最少的PCI

        :param restrict_pci_list: 可选参数，预留PCI列表，用于在规划时排除该PCI
        :return: 返回为这个小区规划的新PCI，如果规划失败则返回False
        """
        if restrict_pci_list is None:
            restrict_pci_list = []
        if not self.can_be_changed:
            print('pci of %s can not be changed' % self.index)
            return -1

        self.gen_pci_rank()

        for pci in list(self.df_pci_rank.sort_values(by=['overlap_rank', 'mod3_rank', 'reuse_rank']).index):
            if pci not in restrict_pci_list and pci >= 0:
                self.set_pci(pci)
                self.pci_plan_in_tiers = 0
                # print(f'return pci: {pci}')
                return pci

        print('Can not pick a better pci for %s' % self.index)
        return -1

    def remove_sss_from_adj_enodeb(self):
        done_list = []
        for adj_cell_index in self.adjacent_cells:
            adj_cell = cells[adj_cell_index]
            adj_enodeb = adj_cell.parent
            if adj_enodeb.enodeb_id not in done_list:
                done_list.append(adj_enodeb.enodeb_id)
                my_sss = math.floor(self.pci / 3)
                for key, group_preferred_sss_list in adj_enodeb.group_preferred_sss.items():
                    if my_sss in group_preferred_sss_list:
                        adj_enodeb.group_preferred_sss[key].remove(my_sss)

    def set_location(self, latitude, longitude):
        """
        为小区设置经纬度，成功返回True，失败返回False
        如果小区有多个经纬度，则依次放入小区经纬度列表中（未实现）
        :param latitude:
        :param longitude:
        :return:
        """
        if check_lat_long(latitude=latitude, longitude=longitude):
            self.longitude = longitude
            self.latitude = latitude
            return True
        else:
            return False

    def set_pci(self, new_pci):
        """
        为小区分配PCI，将该地市PCI统计结果做相应修改
        :param new_pci: 为该小区分配的PCI
        :return: 分配成功返回True，否则返回False
        """
        if not self.can_be_changed:
            logger.error('pci of %s can not be changed' % self.index)
            return False
        if 0 <= new_pci <= MAX_PCI_LTE + 1:
            old_pci = self.pci
            self.pci = new_pci
            self.sss = math.floor(self.pci / 3)
            if new_pci >= 0:
                LteCell.city_arfcn_pci_count[self.city][self.arfcn][new_pci] += 1
                LteCell.city_arfcn_sss_count[self.city][self.arfcn][math.floor(new_pci / 3)] += 1
            if old_pci >= 0:
                LteCell.city_arfcn_pci_count[self.city][self.arfcn][old_pci] -= 1
                LteCell.city_arfcn_sss_count[self.city][self.arfcn][math.floor(old_pci / 3)] -= 1

            # 更新相邻2层小区的可用pci集合属性
            t1 = time.time()
            update_neighbour_available_pci_set(cell_index=self.index, old_pci=old_pci, new_pci=new_pci)
            logger.debug(f'update_neighbour_available_pci_set in {int((time.time() - t1) * 1000)}ms')
            # self.remove_sss_from_adj_enodeb()

            if self.pci != self.org_pci:
                self.pci_changed = True
            else:
                self.pci_changed = False
            return True

        else:
            logger.error('new_pci is invalid: %s' % new_pci)
            return False


class enodeb_cells_group:
    """
    BBU的共址小区组对象
    """

    def __init__(self, **kwargs):
        """
        初始化对象
        :param kwargs:
        """
        self.parent = enodebs[kwargs['enodeb_id']]
        if self.parent:
            self.enodeb_id = kwargs['enodeb_id']
            if kwargs['group_id'] >= 0:
                self.group_id = kwargs['group_id']  # starts from 0
                self.index = f'{self.enodeb_id}_{self.group_id}'  # eNodeBid_groupid
                self.cell_list = kwargs['cell_list']  # [cell对象]
                self.first_tier_groups = []  # 直接相邻的其他组{enodeb_cells_group}
                self.second_tier_groups = []  # 第二层相邻组{enodeb_cells_group}，不包含第1层
                self.third_tier_groups = []  # 第三层相邻组{enodeb_cells_group}，不包含第1，第2层
                self.available_sss = set()  # 本小组可用的sss列表
                self.prefer_sss = []  # 本小组尝试的sss排序列表
                self.is_done = False

    def generate_available_sss(self):
        self.available_sss.clear()
        for cell in self.cell_list:
            self.available_sss.update(cell.available_sss)

    def generate_outer_groups(self):
        """
        生成基站共址小区组的前3层相邻组：

        :return:
        """
        # 计算该组的第1层相邻组：组内每个小区的第一层小区所属的组的去重列表
        for cell in self.cell_list:
            for current_cell_index in cell.first_tier_cells:
                current_cell = cells[current_cell_index]
                if current_cell.parent_group not in self.first_tier_groups:
                    self.first_tier_groups.append(current_cell.parent_group)
        # 计算该组的第2层相邻组：组内每个小区的第2层小区所属的组的去重列表，再剔除已在第1层中的组
        for cell in self.cell_list:
            for current_cell_index in cell.second_tier_cells:
                current_cell = cells[current_cell_index]
                if (current_cell.parent_group not in self.second_tier_groups and
                        current_cell.parent_group not in self.first_tier_groups):
                    self.second_tier_groups.append(current_cell.parent_group)
        # 计算该组的第3层相邻组：组内每个小区的第3层小区所属的组的去重列表，再剔除已在第1,2层中的组
        for cell in self.cell_list:
            for current_cell_index in cell.third_tier_cells:
                current_cell = cells[current_cell_index]
                if (current_cell.parent_group not in self.third_tier_groups and
                        current_cell.parent_group not in self.second_tier_groups and
                        current_cell.parent_group not in self.first_tier_groups):
                    self.second_tier_groups.append(current_cell.parent_group)

    def get_preferred_sss(self, restrict_sss=None, sort_type=0):
        """
                为指定小区组生成优选sss列表
                :param restrict_sss:
                :param sort_type: 指定排序规则
                :return: 返回优选sss列表，失败时返回空列表
                """

        t_get_preferred_sss_start = time.time()
        cell = self.cell_list[0]

        sss_count_series = pd.Series(LteCell.city_arfcn_sss_count[cell.city][cell.arfcn], index=range(MAX_SSS + 1))
        df_sss_candidate = pd.DataFrame(sss_count_series, index=range(MAX_SSS + 1), columns=['total_count'])
        df_sss_candidate['sss_index'] = range(MAX_SSS + 1)

        done_group_list = []
        group_available_sss_set = set(range(MAX_SSS + 1))

        # 生成sss的尝试顺序
        if sort_type == 0:  # 尽量用排在前面的pci
            sss_candidate_list = list(df_sss_candidate.sort_values(by=['sss_index']).index)
        elif sort_type == 1:  # 尽量集中使用pci
            sss_candidate_list = list(df_sss_candidate.sort_values(by=['total_count', 'sss_index']
                                                                   , ascending=[False, True]).index)
        elif sort_type == 2:  # 尽量分散使用pci
            sss_candidate_list = list(df_sss_candidate.sort_values(by=['total_count', 'sss_index']
                                                                   , ascending=[True, True]).index)
        elif 3 <= sort_type <= 6:  # 尽量选用对后续规划影响少的sss，其次考虑分散性，最后考虑序号靠前
            # 对后续规划影响少即：
            # 1、把这个group里面所有小区的前1/2层邻区中仍可用的sss排后面
            # 2、越多邻区可用的sss越靠后
            # 新建字典available_sss_dict和available_sss_dict_3_tier={sss: 可用小区数}
            available_sss_list = []

            available_sss_series = pd.Series([0] * (MAX_SSS + 1), index=range(MAX_SSS + 1))
            for current_group in self.first_tier_groups:
                if current_group and current_group not in done_group_list:
                    done_group_list.append(current_group)
                    available_sss_list.extend(list(current_group.available_sss))

            for current_group in self.second_tier_groups:
                if current_group and current_group not in done_group_list:
                    done_group_list.append(current_group)

                    # weight = (MAX_SSS - len(current_group.available_sss)) * 0.7
                    # series_current_cell_available_sss = pd.Series([weight] * len(current_group.available_sss),
                    #                                               index=list(current_group.available_sss))
                    # available_sss_series += series_current_cell_available_sss
                    available_sss_list.extend(list(current_group.available_sss))

            available_sss_series = pd.Series(available_sss_list)
            available_sss_count_series = available_sss_series.groupby(available_sss_series).count()
            df_sss_candidate['available_count'] = available_sss_count_series

            logger.debug(f'df_sss_candidate of {self.index}: {df_sss_candidate}')

            if sort_type == 3:
                sss_candidate_list = list(df_sss_candidate.sort_values(
                    by=['available_count', 'total_count', 'sss_index'], ascending=[True, True, True]).index)
            elif 4 <= sort_type <= 5:

                sss_candidate_list = list(df_sss_candidate.sort_values(
                    by=['total_count', 'available_count', 'sss_index'], ascending=[True, True, True]).index)
                if sort_type == 5:  # 优先选用3tier可用，但前2层不可用的sss
                    used_sss_in_3rd_tier_list = []
                    used_sss_in_3rd_tier_series = pd.Series([0] * (MAX_SSS + 1), index=range(MAX_SSS + 1))

                    for current_group in self.third_tier_groups:
                        if current_group not in done_group_list:
                            done_group_list.append(current_group)
                            if current_group.is_done:
                                cell = current_group.cell_list[0]
                                current_sss = math.floor(cell.pci / 3)
                                used_sss_in_3rd_tier_list.append(current_sss)
                                used_sss_in_3rd_tier_series[current_sss] += 1
                    # used_sss_in_3rd_tier_series.replace(0, 9999)
                    # 用第3层小区已用sss次数，在对每个前2层可用的sss减去一个固定值，就能使前2层不可用的sss在顺序排时放在前面
                    used_sss_in_3rd_tier_series - pd.Series([9999] * len(set(available_sss_list))
                                                            , index=set(available_sss_list))
                    df_sss_candidate['used_sss_in_3rd_tier'] = used_sss_in_3rd_tier_series
                    # preferred_sss_list = list(set(tmp_preferred_sss_list) - set(available_sss_list))
                    # preferred_sss_list.extend(set(available_sss_list))
                    sss_candidate_list = list(df_sss_candidate.sort_values(
                        by=['used_sss_in_3rd_tier', 'total_count'], ascending=[True, True]).index)

        sss_candidate_set = set(sss_candidate_list)
        sss_candidate_set.intersection_update(group_available_sss_set)
        sss_candidate_set.difference_update(set(restrict_sss))
        return_list = list(sss_candidate_set)
        return_list.sort(key=sss_candidate_list.index)
        t_get_preferred_sss_end = time.time()
        logger.debug(f'get_preferred_sss for {self.index} in '
                     f'{int((t_get_preferred_sss_end - t_get_preferred_sss_start) * 1000)}ms')
        return return_list

    def pick_pci(self, restrict_sss=None, sort_type=3, must_equal_zero=True, try_3_tier=True):
        return_dict = {}
        number_of_cells = len(self.cell_list)
        cells_to_evaluate = number_of_cells

        # 通过集合方式剔除保留PCI对应的sss
        sss_candidate_list = self.get_preferred_sss(sort_type=sort_type, restrict_sss=list(restrict_sss))
        # logger.debug(f'current group:{self.index}, sss_candidate_list:{sss_candidate_list}')
        # 开始规划
        if try_3_tier:
            return_dict = try_assign_sss_to_group_3_tiers(sss_list=sss_candidate_list, cell_list=self.cell_list)
        if len(return_dict) > 0:
            return return_dict

        return_dict = try_assign_sss_to_group(sss_list=sss_candidate_list, cell_list=self.cell_list)

        if len(return_dict) > 0:
            return return_dict
        else:

            if must_equal_zero:
                # print(f'Can not assign pci for group: {self.index} perfectly')
                return {}
            # else:
            #     # 尝试对组内各个小区进行单独规划PCI
            #     print(f'Trying assign pci for group: {self.index} seperatly')
            #     restrict_pci_list = [i * 3 for i in restrict_sss]
            #     restrict_pci_list.extend([i * 3 + 1 for i in restrict_sss])
            #     restrict_pci_list.extend([i * 3 + 2 for i in restrict_sss])
            #     # print(f'restrict_pci_list: {restrict_pci_list}')
            #     for cell in self.cell_list:
            #         pci = cell.pick_pci(restrict_pci_list=restrict_pci_list)
            #         if 0 <= pci <= MAX_PCI_LTE:
            #             return_dict[cell.index] = pci
            #         else:
            #             print(f'Can not assign pci for cell: {cell.index}, mission abort!')
            #             return {}
            #     return return_dict

    def seperate(self):
        """
        用于无法为整组分配pci时，将一个共站组内小区拆分为多个组
        :return: 拆分后的多个group list，或 False

        """
        return_group_list = []
        if len(self.cell_list) > 1:
            i = self.parent.max_group_number + 1
            my_bbu = self.parent
            for my_cell in self.cell_list:
                new_group = enodeb_cells_group(enodeb_id=my_bbu.enodeb_id, group_id=i, cell_list=[my_cell])
                my_bbu.colocation_cells_groups[i] = new_group
                my_bbu.max_group_number = i
                my_cell.parent_group_id = i
                my_cell.parent_group = new_group
                return_group_list.append(my_bbu.colocation_cells_groups[i])
                new_group.generate_outer_groups()
                i += 1
            return return_group_list
        else:
            return False


class Relation:
    cell_a: LteCell = None
    cell_b: LteCell = None
    distance = 0
    overlaps_area = 0
    index = ''

    def __init__(self, *args):
        self.cell_a: LteCell = args[0]
        self.cell_b: LteCell = args[1]
        self.distance = args[2]
        self.overlaps_area = args[3]
        self.index = '%s|%s' % (self.cell_a.index, self.cell_b.index)
