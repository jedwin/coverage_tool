import os
import folium
from folium.plugins import HeatMap
import psycopg2
import pandas as pd
import json
import re

db_host = 'localhost'

db_name = 'postgres'
db_user = 'postgres'
db_password = 'postgres'
db_port = 5432
conn_str = ''
conn = None
engine_str = ''
map_types = {'default': 'OpenStreetMap',
             'gaode_map': 'https://webst01.is.autonavi.com/appmaptile?style=6&x={x}&y={y}&z={z}',
             'gaode_vector': 'http://wprd04.is.autonavi.com/appmaptile?lang=zh_cn&size=1&style=7&x={x}&y={y}&z={z}'}
current_map_type = 'default'
# cities = ['ZS']
cities = ['GZ', 'DG', 'FS', 'HY', 'HZ', 'JM', 'JY', 'MM', 'MZ', 'QY', 'SG',
          'ST', 'SW', 'SZ', 'YF', 'YJ', 'ZH', 'ZJ', 'ZQ', 'CZ', 'ZS']  # 地市列表
cities = {'片区一': ['深圳', '汕头', '潮州', '揭阳', '汕尾'],
          '片区二': ['广州', '中山', '清远', '韶关'],
          '片区三': ['佛山', '阳江', '茂名', '湛江'],
          '片区四': ['东莞', '惠州', '河源', '梅州'],
          '片区五': ['江门', '珠海', '肇庆', '云浮']}
# cities = {'测试区域': ['梅州']}
TYPE_PCI = 'PCI冲突'
TYPE_OVERSHOOT = '越区覆盖'
TYPE_MOD30 = '模30'
TYPE_OVERLAP = '重叠过大'
os_a_area = 100000  # 越区覆盖-本小区面积下限
os_b_area = 30000  # 越区覆盖-相邻小区面积下限
os_st_area = 1200  # 越区覆盖-重叠面积下限
os_b_arfcn = ['75', '100', '1825', '1850']  # 越区覆盖-仅考虑这些频点的相邻小区
os_nei_count_thresh_1 = 50  # 越区覆盖一类地市邻区数量门限
os_nei_count_thresh_2 = 40  # 越区覆盖二类地市邻区数量门限
os_nei_count_thresh_3 = 30  # 越区覆盖除一、二类地市外的邻区数量门限
os_return_limit = 200  # 越区覆盖输出小区地图数量上限
pci_a_area = 12000  # PCI冲突-本小区面积下限
pci_b_area = 12000  # PCI冲突-相邻小区面积下限
pci_st_area = 2000  # PCI冲突-重叠面积下限
pci_neighbor_count = 3  # PCI冲突-邻区数量下限
pci_return_limit = 200  # pci冲突输出小区地图数量上限
ol_a_area = 100000  # 共站重叠算法-本小区面积下限
ol_b_area = 100000  # 共站重叠算法-相邻小区面积下限
ol_st_area_percent = 0.65  # 共站重叠算法-重叠面积占双方比例下限
ol_return_limit = 200  # 共站重叠算法输出小区地图数量上限
mod30_a_area = 30000  # mod30_本小区面积下限
mod30_b_area = 30000  # mod30_相邻小区面积下限
mod30_st_area = 1000  # mod30_重叠面积下限
mod30_return_limit = 200  # mod30_输出小区地图数量上限
conn = psycopg2.connect(host=db_host, port=5432, dbname=db_name,
                        user=db_user, password=db_password)
engine_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
cur = conn.cursor()
tbl_relation = 'cell_relation_gd_20210106'
if current_map_type == 'default':
    tbl_cell_coverage = 'cell_coverage_gd_20210106'
    tbl_cell_info = 'cells_4g_info'
else:
    tbl_cell_coverage = 'cell_coverage_gd_20201114_gcj02'
    tbl_cell_info = 'cells_4g_info_gcj02'

tbl_nr_usage_ratio = 'nr_usage_ratio'
date_of_data = '2021年01月1~7日'
date_of_cell_info_data = 20210106  # 在tbl_cell_info表中day字段的取值
output_folder = '/Users/nathantse/Downloads'
sub_folder = 'results'
json_file_name = f'result_file_{date_of_data}.json'
title = '精准RF优化问题小区清单'
subtitle = '广东综维优中心优化中心数据室'
notice = f'基于{date_of_data}小区MR覆盖多边形数据制作'

make_local = True  # 是否将资源替换成本地


# 将部分外网资源替换为本地资源，以节省载入时间
def replace_local_resouces(in_file):
    SHOULD_REPLACE = 37
    replace_path = '../../resources/'
    origin_pathes = ['https://rawcdn.githack.com/python-visualization/folium/master/folium/templates/',
                     'https://rawcdn.githack.com/ljagis/leaflet-measure/2.1.7/dist/',
                     'https://cdn.jsdelivr.net/npm/leaflet@1.6.0/dist/',
                     'https://code.jquery.com/',
                     'https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/',
                     'https://maxcdn.bootstrapcdn.com/font-awesome/4.6.3/css/',
                     'https://cdnjs.cloudflare.com/ajax/libs/Leaflet.awesome-markers/2.0.2/',
                     'https://cdn.jsdelivr.net/npm/leaflet@1.6.0/dist/',
                     'https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/',
                     'https://cdnjs.cloudflare.com/ajax/libs/Leaflet.awesome-markers/2.0.2/']
    if os.path.exists(in_file):
        replace_count = 0  # 记录共替换了多少个
        replace_pos = 0  # 记录被替换的行号
        with open(in_file, 'r') as o:
            contents = o.readlines()
            o.close()
        #  开始替换本地资源
        for i in range(len(contents)):
            cur_line = contents[i]
            if cur_line.find('<body>') >= 0:
                # print(f'found <body>, replace_count:{replace_count}, replace_pos:{replace_pos}')
                i += 2
                cur_line = contents[i]
                if cur_line.find('<div class="folium-map" id=') >= 0:
                    contents[i] = f'''
                        <div class="bd-duo"  style="height: 100%"><div class="bd-lead" >
                            <div class="notification is-success is-light has-text-centered">
                                {notice}</div>
                        '''
                    contents[i] += cur_line
                    contents[i] += f'''
                        </div><aside class="bd-side"><nav id="anchors" class="bd-anchors is-active bd-content">
                            <p class="bd-anchors-title content is-current">
                            <a onclick="history.go(-1)">返回地市</a></p>
                        </nav></aside></div>
                        '''
                if replace_pos < SHOULD_REPLACE:
                    # 如果被替换的行号少于SHOULD_REPLACE，则需要增加这两个css链接
                    contents.insert(replace_pos + 1, '''    <link rel="stylesheet" href="../../resources/bulma.min.css">
                        ''')
                    contents.insert(replace_pos + 1, '''    <link rel="stylesheet" href="../../resources/bulma-docs.min.css">
                        ''')
                break
            else:
                for origin_path in origin_pathes:
                    if cur_line.find(origin_path) >= 0 or cur_line.find(replace_path) >= 0:
                        contents[i] = cur_line.replace(origin_path, replace_path)
                        replace_count += 1
                        replace_pos = i

        with open(in_file, 'w') as n:
            n.writelines(contents)
            n.close()
            return True
    else:
        return False


# 输出越区覆盖小区
def gen_over_shooting_cells(cities, output_folder, map_tiles='OpenStreetMap'):
    """
    生成越区覆盖小区文件，并返回对应的链接
    :param cities:
    :param output_folder:
    :return:
    """
    return_list = []
    for city in cities:
        print(f'Checking {city} for over shooting cells...')
        if city in ['GZ', 'DG', 'FS', 'SZ', '深圳', '广州', '东莞', '佛山']:
            neighbor_count = os_nei_count_thresh_1
        elif city in ['HZ', 'JM', 'ZH', 'ZS', '中山', '惠州', '江门', '珠海']:
            neighbor_count = os_nei_count_thresh_2
        else:
            neighbor_count = os_nei_count_thresh_3
        sql_string = f'''
        select distinct 
        b.city, b.enodeb_id, b.cell_id, b.arfcn, b.area, num_of_neighbor_cells, st_asgeojson(b.geom), 
        st_x(st_centroid(b.geom)), st_y(st_centroid(b.geom)), 
        c.longitude, c.latitude, b.azimuth, b.cell_name, b.band, b.height    
        from 
        (select city, enodeb_id, cell_id, count(*) num_of_neighbor_cells
        from {tbl_relation} 
        where area > {os_a_area} 
        and b_area > {os_b_area} 
        and st_area > {os_st_area} 
        and arfcn=b_arfcn
        and city = '{city}' 
        and not expanded 
        group by city, enodeb_id, cell_id 
        having count(*) > {neighbor_count} 
        ) a inner join {tbl_cell_coverage} b 
        on (a.enodeb_id = b.enodeb_id and a.cell_id = b.cell_id) 
        left join {tbl_cell_info} c on (a.enodeb_id = c.enodeb_id and a.cell_id = c.cell_id) 
        where c.day={date_of_cell_info_data} 
        order by b.city, num_of_neighbor_cells desc limit {os_return_limit}; 
        '''
        # print(sql_string)
        cur.execute(sql_string)
        results = cur.fetchall()

        # 针对每个越区覆盖小区，输出其有重叠的相邻小区
        i = 1
        print(f'There are {len(results)} results to be handled')
        for result in results:

            r_city = result[0]
            enodeb_id = result[1]
            cell_id = result[2]
            arfcn = result[3]
            area = int(result[4])
            num_of_neighbor_cells = result[5]
            geom = eval(result[6])
            longitude = result[7]
            latitude = result[8]
            cell_long = result[9]
            cell_lat = result[10]
            cell_azimuth = result[11]
            cell_name = result[12]
            cell_band = result[13]
            height = result[14]
            if height == 0:
                height = ' - '
            m = folium.Map([latitude, longitude], zoom_start=15, control_scale=True, tiles=map_tiles,
                           attr=current_map_type,
                           close_popup_on_click=False)  # , tiles='Stamen Terrain', crs='EPSG4326'

            if cell_name is None:
                cell_name = f'{enodeb_id}_{cell_id}'

            sql_string = f'''
            select b_arfcn, st_asgeojson(st_union(b.geom)), count(*) 
            from {tbl_relation} a inner join {tbl_cell_coverage} b 
            on (a.b_enodeb_id = b.enodeb_id and a.b_cell_id = b.cell_id)  
            where a.enodeb_id = {enodeb_id}
            and a.cell_id = {cell_id} 
            and a.st_area > {os_st_area}

            and not expanded 
            group by b_arfcn
            '''
            #     and b_arfcn < 2000 在计算越区时不考虑800m小区，但在出图时考虑
            #     print(sql_string)
            cur.execute(sql_string)
            neighbr_results = cur.fetchall()

            for neighbr_result in neighbr_results:
                nei_arfcn = neighbr_result[0]
                nei_geom = eval(neighbr_result[1])
                nei_count = neighbr_result[2]

                #         nei_city = neighbr_result[0]
                #         nei_enodeb_id = neighbr_result[1]
                #         nei_cell_id = neighbr_result[2]
                #         nei_b_arfcn = neighbr_result[5]
                #         nei_st_area = neighbr_result[6]

                folium.GeoJson(
                    nei_geom,
                    show=False,
                    overlay=True,
                    name=f'{nei_arfcn}频点邻区数量: {nei_count}',
                    tooltip=f'<h5>{nei_arfcn}频点邻区数量: {nei_count}</h5>',
                    style_function=lambda nei_arfcn: {'fillColor': '#0000ff', 'fillOpacity': 0.4, 'weight': 1}
                ).add_to(m)

            html = f'''
                        <h5>越区覆盖小区</h5>
                        小区名称: {cell_name}<br>
                        小区ID: {enodeb_id}_{cell_id} <br>
                        Band: {cell_band} <br>
                        覆盖面积: {area}平方米<br>
                        天线挂高: {height}米
                    '''
            if cell_long is not None and cell_lat is not None:
                # cell_geom = eval(result[15])
                #             folium.GeoJson(
                #                 cell_geom,
                # #                 name=f'{cell_name}',
                #                 tooltip=f'{enodeb_id}_{cell_id}_band {cell_band}: {cell_name}',
                #                 style_function=lambda x: {'fillColor': '#darkblue', 'fillOpacity':1}
                #             ).add_to(m)
                folium.Marker(
                    location=[cell_lat, cell_long],
                    popup=folium.Popup(html=html, max_width=200, auto_close=False),
                    tooltip=folium.Tooltip(text=html, permanent=False),
                    icon=folium.Icon(color='darkblue', icon='fa-wifi', prefix='fa', angle=cell_azimuth)

                ).add_to(m)

            folium.GeoJson(
                geom,
                name=f'{cell_name}',
                #                 tooltip=f'{enodeb_id}_{cell_id}_band {cell_band}: {cell_name}',
                tooltip=html,
                style_function=lambda x: {'fillColor': 'lightblue', 'fillOpacity': 0.8}
            ).add_to(m)
            folium.map.LayerControl().add_to(m)
            folium.plugins.MeasureControl().add_to(m)
            city_folder = f'{city}_{date_of_data}'
            if not os.path.exists(os.path.join(output_folder, sub_folder, city_folder)):
                os.mkdir(os.path.join(output_folder, sub_folder, city_folder))
            link_path = os.path.join(city_folder, 'overshoot')
            path = os.path.join(output_folder, sub_folder, city_folder, 'overshoot')
            file_name = f'{city}_{i}_{enodeb_id}_{cell_id}.html'
            if not os.path.exists(path):
                os.mkdir(path)
            save_file = os.path.join(path, file_name)
            file_link = os.path.join(link_path, file_name)
            m.save(save_file)
            if make_local:
                if not replace_local_resouces(save_file):
                    print(f'can not modify the output file: {save_file}')
            return_list.append([city, '越区覆盖', i, f'{cell_name}', num_of_neighbor_cells, file_link])
            i += 1
    print('Done!')
    return return_list


# PCI混淆
def gen_pci_conflict_cells(cities, output_folder, map_tiles='OpenStreetMap'):
    """
    生成越区覆盖小区文件，并返回对应的链接
    :param cities:
    :param output_folder:
    :return:
    """
    return_list = []
    for city in cities:
        print(f'Checking {city} for PCI conflict cells...')
        sql_string = f'''
            select distinct g1.city, cell.b_enodeb_id enodeb_id,  cell.b_cell_id cell_id, cell.b_arfcn arfcn, cell.b_pci pci,  
            st_asgeojson(g1.geom) geom1,  
            g2.city city_2, cell2.b_enodeb_id enodeb_id_2 , cell2.b_cell_id cell_id_2, st_asgeojson(g2.geom) geom2, 
            ST_Distance(g1.geom::geography, g2.geom::geography) coverage_distance,
            st_distance(st_centroid(g1.geom)::geography, st_centroid(g2.geom)::geography) cell_distance,
            count(*) affected_cells,
            st_x(st_centroid(ST_MakeLine(st_centroid(g1.geom), st_centroid(g2.geom)))), 
            st_y(st_centroid(ST_MakeLine(st_centroid(g1.geom), st_centroid(g2.geom)))), 
            c.longitude, c.latitude, g1.azimuth, g1.cell_name, g1.band, 
            c2.longitude, c2.latitude, g2.azimuth, g2.cell_name, g2.band   
            from {tbl_relation} cell inner join {tbl_cell_coverage} g1 on (cell.b_enodeb_id = g1.enodeb_id
            And cell.b_cell_id =g1.cell_id)
            left join {tbl_cell_info} c on (cell.b_enodeb_id = c.enodeb_id and cell.b_cell_id = c.cell_id), 
            {tbl_relation} cell2 inner join {tbl_cell_coverage} g2 on (cell2.b_enodeb_id = g2.enodeb_id
            And cell2.b_cell_id =g2.cell_id) 
            left join {tbl_cell_info} c2 on (cell2.b_enodeb_id = c2.enodeb_id and cell2.b_cell_id = c2.cell_id) 
            Where cell2.enodeb_id = cell.enodeb_id
            and cell2.cell_id =cell.cell_id
            and cell2.b_pci = cell.b_pci 
            and (cell2.b_enodeb_id != cell.b_enodeb_id or cell2.b_cell_id != cell.b_cell_id)
            and cell.b_arfcn=cell2.b_arfcn
            and cell.area >= {pci_a_area}
            and cell2.area >= {pci_b_area}
            and cell.st_area >= {pci_st_area}
            and cell2.st_area >= {pci_st_area}
            and not cell.expanded
            and not cell2.expanded
            and (g1.city = '{city}' or g2.city = '{city}')
            and c.day={date_of_cell_info_data} and c2.day={date_of_cell_info_data} 
            group by g1.city, cell.b_enodeb_id,  cell.b_cell_id, cell.b_arfcn, cell.b_pci
            , g1.geom
            , g2.city, cell2.b_enodeb_id, cell2.b_cell_id, coverage_distance
            , cell_distance, g2.geom,
            c.longitude, c.latitude, g1.azimuth, g1.cell_name, g1.band, 
            c2.longitude, c2.latitude, g2.azimuth, g2.cell_name, g2.band 
            having count(*) > {pci_neighbor_count} 
            order by affected_cells desc, pci 
            limit {pci_return_limit};
        '''
        #     print(sql_string)
        cur.execute(sql_string)
        results = cur.fetchall()

        # 针对每对PCI混淆小区，输出它们的共同相邻小区

        cell_pairs = []  # 存放已输出的混淆对，防止重复输出
        i = 1
        print(f'There are {len(results)} results to be handled')
        for result in results:
            city1 = result[0]
            enodeb_id = result[1]
            cell_id = result[2]
            arfcn = result[3]
            pci = result[4]
            geom1 = eval(result[5])
            city2 = result[6]
            enodeb2_id = result[7]
            cell2_id = result[8]
            geom2 = eval(result[9])
            coverage_distance = result[10]
            cell_distance = result[11]
            affected_cells = result[12]
            longitude = result[13]
            latitude = result[14]

            cell_long = result[15]
            cell_lat = result[16]
            cell_azimuth = result[17]
            cell_name = result[18]
            cell_band = result[19]

            cell2_long = result[20]
            cell2_lat = result[21]
            cell2_azimuth = result[22]
            cell2_name = result[23]
            cell2_band = result[24]
            m = folium.Map([latitude, longitude], zoom_start=15, tiles=map_tiles, attr=current_map_type,
                           close_popup_on_click=False)  # , tiles='Stamen Terrain', crs='EPSG4326'

            cell1 = f'{enodeb_id}_{cell_id}'
            cell2 = f'{enodeb2_id}_{cell2_id}'
            if cell_name is None:
                cell_name = cell1
            if cell2_name is None:
                cell2_name = cell2
            if [cell1, cell2] not in cell_pairs and [cell2, cell1] not in cell_pairs:
                cell_pairs.append([cell1, cell2])

                sql_string = f'''
                    select distinct cell.city, cell.enodeb_id, cell.cell_id, st_asgeojson(g.geom), count(*),
                    c.longitude, c.latitude, g.azimuth, g.cell_name, g.band 
                    from {tbl_relation} cell inner join {tbl_cell_coverage} g 
                    on (cell.enodeb_id = g.enodeb_id And cell.cell_id =g.cell_id) 
                    left join {tbl_cell_info} c on (g.enodeb_id = c.enodeb_id and g.cell_id = c.cell_id) 
                    where ((b_enodeb_id={enodeb_id} and b_cell_id={cell_id}) 
                    or (b_enodeb_id={enodeb2_id} and b_cell_id={cell2_id})) 
                    and cell.st_area > {pci_st_area} 
                    and cell.area >= {pci_b_area}
                    and c.day={date_of_cell_info_data} 
                    group by cell.city, cell.enodeb_id, cell.cell_id, st_asgeojson(g.geom), 
                    c.longitude, c.latitude, g.azimuth, g.cell_name, g.band
                    having count(*) > 1 

                '''

                #             print(sql_string)
                cur.execute(sql_string)
                neighbr_results = cur.fetchall()
                # 输出它们的共同相邻小区
                j = 0
                for neighbr_result in neighbr_results:
                    j += 1
                    nei_city = neighbr_result[0]
                    nei_enodeb_id = neighbr_result[1]
                    nei_cell_id = neighbr_result[2]
                    nei_geom = eval(neighbr_result[3])
                    nei_count = neighbr_result[4]
                    nei_long = neighbr_result[5]
                    nei_lat = neighbr_result[6]
                    nei_azimuth = neighbr_result[7]
                    nei_name = neighbr_result[8]
                    nei_band = neighbr_result[9]

                    html = f'''
                            <h5>受影响小区{j}</h5>
                            小区ID：{nei_enodeb_id}_{nei_cell_id}<br>
                            小区名称{nei_name}'''
                    if nei_long is not None and nei_lat is not None:
                        folium.Marker(
                            location=[nei_lat, nei_long],
                            popup=folium.Popup(html=html, max_width=200, auto_close=False),

                            icon=folium.Icon(color='lightblue', icon='fa-wifi', prefix='fa', angle=nei_azimuth)
                        ).add_to(m)
                    folium.GeoJson(
                        nei_geom,
                        show=False,
                        overlay=True,
                        name=f'受影响小区{j}:{nei_city}_{nei_name}',
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        style_function=lambda nei_arfcn: {'fillColor': '#0000ff', 'fillOpacity': 0.3, 'weight': 1}
                    ).add_to(m)

                html = f'''
                            <h5>PCI冲突小区1</h5>
                            小区名称: {cell_name} <br>
                            地市: {city1} <br>
                            小区ID: {cell1} <br>
                            Band: {cell_band} <br>
                            PCI: {pci} <br>
                            受影响小区数: {affected_cells}
                            '''
                if cell_long is not None and cell_lat is not None:
                    folium.Marker(
                        location=[cell_lat, cell_long],
                        popup=folium.Popup(html=html, max_width=200, auto_close=False),
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        icon=folium.Icon(color='darkgreen', icon='fa-wifi', prefix='fa', angle=cell_azimuth)
                    ).add_to(m)
                folium.GeoJson(
                    geom1,
                    name=f'CELL1: {cell_name}',
                    tooltip=html,
                    style_function=lambda x: {'fillColor': '#00f000', 'fillOpacity': 0.9}
                ).add_to(m)

                html = f'''
                            <h5>PCI冲突小区2</h5>
                            小区名称: {cell2_name} <br>
                            地市: {city2} <br>
                            小区ID: {cell2} <br>
                            Band: {cell2_band} <br>
                            PCI: {pci} <br>
                            受影响小区数: {affected_cells}
                            '''
                if cell2_long is not None and cell2_lat is not None:
                    folium.Marker(
                        location=[cell2_lat, cell2_long],
                        popup=folium.Popup(html=html, max_width=200, auto_close=False),
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        icon=folium.Icon(color='orange', icon='fa-wifi', prefix='fa', angle=cell2_azimuth)

                    ).add_to(m)
                folium.GeoJson(
                    geom2,
                    name=f'CELL2: {cell2_name}',
                    tooltip=html,
                    style_function=lambda x: {'fillColor': '#f0e000', 'fillOpacity': 0.9}
                ).add_to(m)
                folium.map.LayerControl().add_to(m)
                folium.plugins.MeasureControl().add_to(m)
                city_folder = f'{city}_{date_of_data}'
                if not os.path.exists(os.path.join(output_folder, sub_folder, city_folder)):
                    os.mkdir(os.path.join(output_folder, sub_folder, city_folder))
                link_path = os.path.join(city_folder, 'pci_conflict')
                path = os.path.join(output_folder, sub_folder, city_folder, 'pci_conflict')
                file_name = f'{city}_{i}_{cell1}_and_{cell2}.html'
                if not os.path.exists(path):
                    os.mkdir(path)
                save_file = os.path.join(path, file_name)
                file_link = os.path.join(link_path, file_name)
                m.save(save_file)
                if make_local:
                    if not replace_local_resouces(save_file):
                        print(f'can not modify the output file: {save_file}')
                return_list.append([city, 'PCI冲突', i, f'{cell_name}, {cell2_name}', affected_cells, file_link])
                i += 1

    print('Done!')
    return return_list


# 输出同站同频，且重叠比例过大的小区
def gen_overlap_cells(cities, output_folder, map_tiles='OpenStreetMap'):
    """

    :param cities:
    :param output_folder:
    :return:
    """
    return_list = []
    for city in cities:
        print(f'Checking {city} for overlap cell pairs...')
        sql_string = f'''
                select distinct cell.city, cell.enodeb_id,  cell.cell_id, cell.arfcn, cell.pci,  
                st_asgeojson(g1.geom) geom1,  
                cell.b_city city_2, cell.b_enodeb_id enodeb_id_2 , cell.b_cell_id cell_id_2, cell.b_pci pci_2,
                st_asgeojson(g2.geom) geom2, 
                (cell.st_area / cell.area) * 100 percent1,
                (cell.st_area / cell.b_area) * 100 percent2,
                st_x(st_centroid(ST_MakeLine(st_centroid(g1.geom), st_centroid(g2.geom)))), 
                st_y(st_centroid(ST_MakeLine(st_centroid(g1.geom), st_centroid(g2.geom)))), 
                c.longitude, c.latitude, g1.azimuth, g1.cell_name, g1.band, 
                c2.longitude, c2.latitude, g2.azimuth, g2.cell_name, g2.band   
                from {tbl_relation} cell inner join {tbl_cell_coverage} g1 on (cell.enodeb_id = g1.enodeb_id
                And cell.cell_id =g1.cell_id) 
                left join {tbl_cell_info} c on (cell.enodeb_id = c.enodeb_id and cell.cell_id = c.cell_id)  
                inner join {tbl_cell_coverage} g2 on (cell.b_enodeb_id = g2.enodeb_id
                And cell.b_cell_id =g2.cell_id) 
                left join {tbl_cell_info} c2 on (cell.b_enodeb_id = c2.enodeb_id and cell.b_cell_id = c2.cell_id) 
                Where cell.arfcn=cell.b_arfcn
                and cell.enodeb_id = cell.b_enodeb_id 
                and cell.area >= {ol_a_area}
                and cell.b_area >= {ol_b_area}
                and cell.st_area / cell.area >= {ol_st_area_percent} 
                and cell.st_area / cell.b_area >= {ol_st_area_percent} 
                and not cell.expanded 
                and (cell.city = '{city}' or cell.b_city = '{city}')
                and c.day={date_of_cell_info_data} and c2.day={date_of_cell_info_data} 
                order by percent1 desc, percent2 desc, cell.enodeb_id,  cell.cell_id
                limit {ol_return_limit};
                '''

        #         print(sql_string)
        cur.execute(sql_string)
        results = cur.fetchall()

        cell_pairs = []  # 存放已输出的混淆对，防止重复输出
        i = 1
        print(f'There are {len(results)} results to be handled')
        for result in results:
            city1 = result[0]
            enodeb_id = result[1]
            cell_id = result[2]
            arfcn = result[3]
            pci = result[4]
            geom1 = eval(result[5])
            city2 = result[6]
            enodeb2_id = result[7]
            cell2_id = result[8]
            pci2 = result[9]
            geom2 = eval(result[10])
            percent1 = int(result[11])
            percent2 = int(result[12])
            longitude = result[13]
            latitude = result[14]

            cell_long = result[15]
            cell_lat = result[16]
            cell_azimuth = result[17]
            cell_name = result[18]
            cell_band = result[19]

            cell2_long = result[20]
            cell2_lat = result[21]
            cell2_azimuth = result[22]
            cell2_name = result[23]
            cell2_band = result[24]

            m = folium.Map([latitude, longitude], zoom_start=15, control_scale=True, tiles=map_tiles,
                           attr=current_map_type,
                           close_popup_on_click=False)  # , tiles='Stamen Terrain', crs='EPSG4326'
            cell1 = f'{enodeb_id}_{cell_id}'
            cell2 = f'{enodeb2_id}_{cell2_id}'
            if cell_name is None:
                cell_name = cell1
            if cell2_name is None:
                cell2_name = cell2
            if [cell1, cell2] not in cell_pairs and [cell2, cell1] not in cell_pairs:
                cell_pairs.append([cell1, cell2])
                if cell_long is not None and cell_lat is not None:

                    html = f'''
                            <h5>共站重叠过大小区1</h5>
                            地市: {city1} <br>
                            小区ID: {cell1} <br>
                            小区名称: {cell_name}<br>
                            Band: {cell_band} <br>
                            重叠比例: {percent1}%<br>
                            方位角: {cell_azimuth}
                        '''
                    folium.Marker(
                        location=[cell_lat, cell_long],
                        popup=folium.Popup(html=html, max_width=200, auto_close=False),
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        icon=folium.Icon(color='darkgreen', icon='fa-wifi', prefix='fa', angle=cell_azimuth)

                    ).add_to(m)

                    folium.GeoJson(
                        geom1,
                        name=f'CELL1: {cell_name}',
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        popup=folium.Popup(html=html, max_width=150, auto_close=False),
                        style_function=lambda x: {'fillColor': '#00f000', 'fillOpacity': 0.7}
                    ).add_to(m)
                    if cell2_long is not None and cell2_lat is not None:
                        html = f'''
                                 <h5>共站重叠过大小区2</h5>
                                地市: {city2} <br>
                                小区ID: {cell2} <br>
                                小区名称: {cell2_name}<br>
                                Band: {cell2_band} <br>
                                重叠比例: {percent2}%<br>
                                方位角: {cell2_azimuth}
                            '''
                        folium.Marker(
                            location=[cell2_lat, cell2_long],
                            popup=folium.Popup(html=html, max_width=150, auto_close=False),
                            tooltip=folium.Tooltip(text=html, permanent=False),
                            icon=folium.Icon(color='orange', icon='fa-wifi', prefix='fa', angle=cell2_azimuth)

                        ).add_to(m)
                    folium.GeoJson(
                        geom2,
                        name=f'CELL2: {cell2_name}',
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        popup=folium.Popup(html=html, max_width=150, auto_close=False),
                        style_function=lambda x: {'fillColor': '#f0e000', 'fillOpacity': 0.7}
                    ).add_to(m)

                folium.map.LayerControl().add_to(m)
                folium.plugins.MeasureControl().add_to(m)
                city_folder = f'{city}_{date_of_data}'
                if not os.path.exists(os.path.join(output_folder, sub_folder, city_folder)):
                    os.mkdir(os.path.join(output_folder, sub_folder, city_folder))
                link_path = os.path.join(city_folder, 'overlap_cells')
                path = os.path.join(output_folder, sub_folder, city_folder, 'overlap_cells')
                file_name = f'{city}_{i}_{cell1}_and_{cell2}.html'
                if not os.path.exists(path):
                    os.mkdir(path)
                save_file = os.path.join(path, file_name)
                file_link = os.path.join(link_path, file_name)
                m.save(save_file)
                if make_local:
                    if not replace_local_resouces(save_file):
                        print(f'can not modify the output file: {save_file}')

                return_list.append([city, '重叠过大', i, f'{cell_name}, {cell2_name}', percent1, file_link])
                i += 1
    print('Done!')
    return return_list


# 输出模30相同的重叠小区
def gen_mod30_cells(cities, output_folder, map_tiles='OpenStreetMap'):
    """

    :param cities:
    :param output_folder:
    :return:
    """
    return_list = []
    for city in cities:
        print(f'Checking {city} for mod 30 cell pairs...')
        sql_string = f'''
            select distinct cell.city, cell.enodeb_id,  cell.cell_id, cell.arfcn, cell.pci,  
            st_asgeojson(g1.geom) geom1,  
            cell.b_city city_2, cell.b_enodeb_id enodeb_id_2 , cell.b_cell_id cell_id_2, cell.b_pci pci_2,
            st_asgeojson(g2.geom) geom2, 
            cell.st_area,
            st_distance(c.geom::geography, c2.geom::geography) cell_distance,
            st_x(st_centroid(ST_MakeLine(st_centroid(g1.geom), st_centroid(g2.geom)))), 
            st_y(st_centroid(ST_MakeLine(st_centroid(g1.geom), st_centroid(g2.geom)))), 
            c.longitude, c.latitude, g1.azimuth, g1.cell_name, g1.band, 
            c2.longitude, c2.latitude, g2.azimuth, g2.cell_name, g2.band_number   
            from {tbl_relation} cell inner join {tbl_cell_coverage} g1 on (cell.enodeb_id = g1.enodeb_id
            And cell.cell_id =g1.cell_id) 
            left join {tbl_cell_info} c on (cell.enodeb_id = c.enodeb_id and cell.cell_id = c.cell_id)  
            inner join {tbl_cell_coverage} g2 on (cell.b_enodeb_id = g2.enodeb_id
            And cell.b_cell_id =g2.cell_id) 
            left join {tbl_cell_info} c2 on (cell.b_enodeb_id = c2.enodeb_id and cell.b_cell_id = c2.cell_id) 
            Where cell.pci % 30 = cell.b_pci % 30
            and cell.arfcn=cell.b_arfcn
            and cell.area >= {mod30_a_area}
            and cell.b_area >= {mod30_b_area}
            and cell.st_area >= {mod30_st_area} 
            and not cell.expanded 
            and (cell.city = '{city}' or cell.b_city = '{city}') 
            and c.day={date_of_cell_info_data} and c2.day={date_of_cell_info_data} 
            order by cell.st_area desc, cell_distance, cell.enodeb_id,  cell.cell_id
            limit {mod30_return_limit};
            '''

        #         print(sql_string)
        cur.execute(sql_string)
        results = cur.fetchall()

        cell_pairs = []  # 存放已输出的混淆对，防止重复输出
        i = 1
        print(f'There are {len(results)} results to be handled')
        for result in results:
            city1 = result[0]
            enodeb_id = result[1]
            cell_id = result[2]
            arfcn = result[3]
            pci = result[4]
            geom1 = eval(result[5])
            city2 = result[6]
            enodeb2_id = result[7]
            cell2_id = result[8]
            pci2 = result[9]
            geom2 = eval(result[10])
            ol_area = int(result[11])
            cell_distance = result[12]
            longitude = result[13]
            latitude = result[14]

            cell_long = result[15]
            cell_lat = result[16]
            cell_azimuth = result[17]
            cell_name = result[18]
            cell_band = result[19]

            cell2_long = result[20]
            cell2_lat = result[21]
            cell2_azimuth = result[22]
            cell2_name = result[23]
            cell2_band = result[24]

            m = folium.Map([latitude, longitude], zoom_start=15, control_scale=True, tiles=map_tiles,
                           attr=current_map_type,
                           close_popup_on_click=False)  # , tiles='Stamen Terrain', crs='EPSG4326'
            cell1 = f'{enodeb_id}_{cell_id}'
            cell2 = f'{enodeb2_id}_{cell2_id}'
            if cell_name is None:
                cell_name = cell1
            if cell2_name is None:
                cell2_name = cell2
            if [cell1, cell2] not in cell_pairs and [cell2, cell1] not in cell_pairs:
                cell_pairs.append([cell1, cell2])
                if cell_long is not None and cell_lat is not None:

                    html = f'''
                        <h4>Mod30 小区1</h4><br>
                        City: {city1} <br>
                        cell: {cell1} <br>
                        cellname: {cell_name}<br>
                        ARFCN: {arfcn} <br>
                        overlap: {ol_area}m2<br>
                        PCI: {pci} <br>
                        Mod30：{pci % 30}
                    '''
                    folium.Marker(
                        location=[cell_lat, cell_long],
                        popup=folium.Popup(html=html, max_width=200, auto_close=False),
                        tooltip=folium.Tooltip(text=html, permanent=True),
                        icon=folium.Icon(color='red', icon='fa-wifi', prefix='fa', angle=cell_azimuth)

                    ).add_to(m)

                    folium.GeoJson(
                        geom1,
                        name=f'CELL1: {cell_name}',
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        popup=folium.Popup(html=html, max_width=150, auto_close=False),
                        style_function=lambda x: {'fillColor': '#00f000', 'fillOpacity': 0.7}
                    ).add_to(m)
                    if cell2_long is not None and cell2_lat is not None:
                        html = f'''
                            <h4>Mod30 小区2</h4><br>
                            City: {city2} <br>
                            cell: {cell2} <br>
                            cellname: {cell2_name}<br>
                            ARFCN: {arfcn} <br>
                            overlap: {ol_area}m2<br>
                            PCI: {pci2} <br>
                            Mod30：{pci2 % 30}
                        '''
                        folium.Marker(
                            location=[cell2_lat, cell2_long],
                            popup=folium.Popup(html=html, max_width=150, auto_close=False),
                            tooltip=folium.Tooltip(text=html, permanent=True),
                            icon=folium.Icon(color='orange', icon='fa-wifi', prefix='fa', angle=cell2_azimuth)

                        ).add_to(m)
                    folium.GeoJson(
                        geom2,
                        name=f'CELL2: {cell2_name}',
                        tooltip=folium.Tooltip(text=html, permanent=False),
                        popup=folium.Popup(html=html, max_width=150, auto_close=False),
                        style_function=lambda x: {'fillColor': '#f0e000', 'fillOpacity': 0.7}
                    ).add_to(m)

                folium.map.LayerControl().add_to(m)
                folium.plugins.MeasureControl().add_to(m)
                city_folder = f'{city}_{date_of_data}'
                if not os.path.exists(os.path.join(output_folder, sub_folder, city_folder)):
                    os.mkdir(os.path.join(output_folder, sub_folder, city_folder))
                link_path = os.path.join(city_folder, 'mod30')
                path = os.path.join(output_folder, sub_folder, city_folder, 'mod30')
                file_name = f'{city}_{i}_{cell1}_and_{cell2}.html'
                if not os.path.exists(path):
                    os.mkdir(path)
                save_file = os.path.join(path, file_name)
                file_link = os.path.join(link_path, file_name)
                m.save(save_file)
                if make_local:
                    if not replace_local_resouces(save_file):
                        print(f'can not modify the output file: {save_file}')

                return_list.append([city, '模30', i, f'{cell_name}, {cell2_name}', ol_area, file_link])
                i += 1
    print('Done!')
    return return_list


def heatmap_nr_planning():
    return_list = []
    sql_string = f'''
    select st_y(st_centroid(a.geom)) lat, st_x(st_centroid(a.geom)) long, 
    avg(b.nr_data_ratio) 
    from cell_coverage_gd_202008 a inner join nr_usage_ratio b 
    on (a.enodeb_id = b.enodeb_id and a.cell_id = b.cell_id) 
    group by st_y(st_centroid(a.geom)), st_x(st_centroid(a.geom)) 
    having avg(b.nr_data_ratio) > 0
    order by avg(b.nr_data_ratio) desc 

    '''
    cur.execute(sql_string)
    results = cur.fetchall()

    m = folium.Map([results[0][0], results[0][1]], zoom_start=15, control_scale=True,
                   close_popup_on_click=False)

    HeatMap(data=results, name='5G驻留比', max_val=100,
            blur=25, min_opacity=0.5, radius=15,
            gradient={0.2: 'red', 0.3: 'orange', 0.45: 'yellow', 0.6: 'lightgreen', 0.7: 'green', 0.8: 'blue'},
            control=True).add_to(m)

    sql_string = f'''
    select a.latitude, a.longitude, a.cell_name,
    avg(b.nsa_user) avg_users, 
    avg(nr_data_ratio) avg_nr_data_ratio, 
    avg(nr_time_ratio) avg_nr_time_ratio
    from cell_info_20200815 a inner join nr_usage_ratio b 
    on (a.enodeb_id = b.enodeb_id and a.cell_id = b.cell_id) 
    where b.nsa_user > 10
    group by a.latitude, a.longitude, a.cell_name
    having (avg(b.nr_data_ratio) < 0.5 and avg(nr_time_ratio) < 0.5)
    order by avg(b.nsa_user) desc 

    '''
    cur.execute(sql_string)
    results = cur.fetchall()
    heatmap_data = []
    for result in results:
        cell_lat = result[0]
        cell_long = result[1]
        cell_name = result[2]
        avg_nsa_user = int(result[3])
        avg_nr_data_ratio = int(result[4] * 100) / 100
        avg_nr_time_ratio = int(result[5] * 100) / 100
        heatmap_data.append([cell_lat, cell_long, avg_nsa_user])
        html = f'''
            <h5>多NSA用户低驻留比小区</h5><br>
            cellname: {cell_name}<br>
            5G流量驻留比: {avg_nr_data_ratio}%<br>
            5G时长驻留比: {avg_nr_time_ratio}%<br>
            NSA用户数: {avg_nsa_user} <br>
        '''
        folium.Marker(
            location=[cell_lat, cell_long],
            popup=folium.Popup(html=html, max_width=200, auto_close=False),
            tooltip=folium.Tooltip(text=html, permanent=False),
            icon=folium.Icon(color='blue')
        ).add_to(m)

    HeatMap(data=heatmap_data, name='NSA用户数', max_val=1,
            gradient={0.4: 'yellow', 0.6: 'lime', 0.8: 'red'},
            control=True).add_to(m)
    folium.map.LayerControl().add_to(m)
    path = os.path.join(sub_folder, 'heatmap')
    if not os.path.exists(path):
        os.mkdir(path)
    save_file = os.path.join(path, f'zs.html')
    m.save(save_file)
    return_list.append(save_file)
    print('Done')
    return return_list


# choropleth map
def choropleth_map():
    sql_string = f'''
    select a.enodeb_id, a.cell_id, st_asgeojson(a.geom) cell_geom,  
    avg(b.nr_data_ratio), a.arfcn 
    from cell_coverage_gd_202008 a inner join nr_usage_ratio b 
    on (a.enodeb_id = b.enodeb_id and a.cell_id = b.cell_id) 
    where a.arfcn=100
    group by a.enodeb_id, a.cell_id, st_asgeojson(a.geom) ,a.arfcn 

    order by avg(b.nr_data_ratio) desc 
    limit 10
    '''
    cur.execute(sql_string)
    results = cur.fetchall()
    geo_dict = {}
    geo_dict['type'] = 'FeatureCollection'
    geo_dict['features'] = []
    nr_dict = {}
    nr_data = []
    for result in results:
        enodeb_id = result[0]
        cell_id = result[1]
        cell_geom = result[2]
        eci = f'{enodeb_id}_{cell_id}'
        nr_data_ratio = int(result[3] * 100) / 100
        arfcn = result[4]
        geo_dict['features'].append(
            {'type': 'Feature', 'id': eci, 'properties': {'arfcn': arfcn, 'eci': eci}, 'geometry': cell_geom})
        nr_dict[eci] = nr_data_ratio
        nr_data.append([eci, nr_data_ratio])
    nr_series = pd.Series(nr_dict)
    df_nr_usage = pd.DataFrame(nr_data, index=nr_dict.keys(), columns=['eci', 'nr_data_ratio'])
    m = folium.Map([22.4, 113.3], zoom_start=10, control_scale=True,
                   close_popup_on_click=False)
    folium.Choropleth(geo_data=geo_dict, name='choropleth map',
                      data=df_nr_usage,
                      columns=['eci', 'nr_data_ratio'],
                      key_on='feature.properties.eci',
                      fill_color='YlGn', fill_opacity=0.7,
                      line_opacity=0.2, legend_name='data_ratio (%)'
                      ).add_to(m)
    folium.map.LayerControl().add_to(m)
    path = os.path.join(sub_folder, 'choropleth')
    if not os.path.exists(path):
        os.mkdir(path)
    save_file = os.path.join(path, f'zs_choropleth.html')
    m.save(save_file)
    print('Done')


def gen_province_page(map_file_lists, summary_file_name_prefix, output_folder):
    """

    :return:
    """
    province_file_name = f'全省-{summary_file_name_prefix}-{date_of_data}.html'
    province_file = os.path.join(output_folder, sub_folder, province_file_name)
    html_header = f'''
            <!DOCTYPE html>
            <html>
              <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>{title}--广东</title>
                <link rel="stylesheet" href="resources/bulma.min.css">
                <link rel="stylesheet" href="resources/bulma-docs.min.css">
                <script defer src="resources/all.js"></script>
              </head>
              <body>
              
        '''
    html_footer = f'''
            <footer class="footer">
              <div class="content has-text-centered">
                <p>
                  该网页CSS使用 <a href="https://jgthms.com">Jeremy Thomas</a>编写的<strong>Bulma</strong> . 
                    其源代码使用<a href="http://opensource.org/licenses/mit-license.php">MIT</a>许可证. 
                </p>
              </div>
            </footer>
        '''
    html_tail = f'''
            </body>
        </html>
        '''
    with open(province_file, 'w') as n:
        contents = html_header
        contents += f'<div class="bd-duo"><div class="bd-lead"><section class="section">'
        contents += f'''
                <div class="container">
                  <h1 class="title has-text-centered">
                    {title}--广东
                  </h1>
                  <p class="subtitle has-text-centered">
                    {subtitle} 
                  </p>
                </div>
                <div class="notification is-success is-light has-text-centered">
                {notice}
                </div>
            '''
        contents += f'''
            <div class="tile is-ancestor">
                <div class="tile is-parent is-4">
                    <article class="message is-link tile is-child">
                        <div class="message-header">越区覆盖判断条件</div>
                        <div div class="message-body">
                            <li>小区自身覆盖面积大于<code>{os_a_area}</code>平方米</li>
                            <li>该小区与超过<code>N</code>个覆盖面积大于<code>{os_b_area}</code>平方米的
                            <span class="has-text-danger-dark">同频</span>相邻小区有重叠覆盖
                            且这些重叠部分面积均超过<code>{os_st_area}</code>平方米</li> 
                            <li>在深圳、广州、东莞、佛山<code>N={os_nei_count_thresh_1}</code>，
                            在江门、珠海、中山、惠州<code>N={os_nei_count_thresh_2}</code>，
                            在其余地市<code>N={os_nei_count_thresh_3}</code></li>
                            <li>仅计算<code>{str.replace(str(os_b_arfcn)[1:-1], "'", "")}</code>频点小区</li>
                            <li>返回严重程度最高的前<code>{os_return_limit}</code>个结果</li>
                        </div>
                    </article>
                </div>
                <div class="tile is-parent is-4">
                    <article class="message is-link tile is-child">
                        <div class="message-header">PCI冲突判断条件</div>
                        <div div class="message-body">
                        <li>小区1自身覆盖面积大于<code>{pci_a_area}</code>平方米</li>
                        <li>小区2自身覆盖面积大于<code>{pci_b_area}</code>平方米</li>
                        <li>冲突小区覆盖重叠面积大于<code>{pci_st_area}</code>平方米</li>
                        <li>同时覆盖这两个同PCI小区的受影响小区数量大于<code>{pci_neighbor_count}</code>个</li>
                        <li>返回严重程度最高的前<code>{pci_return_limit}</code>个相关小区</li>
                        </div>
                    </article>
                </div>
                <div class="tile is-parent is-4">
                    <article class="message is-link tile is-child">
                        <div class="message-header">共站同频小区重叠比例过大判断条件</div>
                        <div div class="message-body">
                         <li>小区1自身覆盖面积均大于<code>{ol_a_area}</code>平方米</li>
                        <li>小区2自身覆盖面积均大于<code>{ol_b_area}</code>平方米</li>
                        <li>两个小区<code>eNodeB id</code>和<code>频点</code>相同</li>
                        <li>两个小区覆盖重叠面积占各自面积比例均大于<code>{int(ol_st_area_percent * 100)}%</code></li>
                        <li>返回严重程度最高的前<code>{ol_return_limit}</code>个相关小区</li>
                        </div>
                    </article>
                </div>
            </div>
            '''
        # contents += f'<div class="hero-body"><div class="container">'
        contents_aside = f'''<aside class="bd-side">
                    <nav id="anchors" class="bd-anchors is-active bd-content">
                        <p class="bd-anchors-title content">全省问题清单</p>
                        <ul class="bd-anchors-list content">
                    '''
        for area, area_cities in cities.items():
            contents_aside += f'''<li>{area}<ul>'''
            for city in area_cities:
                problem_lists = map_file_lists[city]
                contents += f'''
                    <h1 id="{city}" class="title">
                        {city}
                    </h1>
                    <div class="bd-structure" style="font-size: 20px">
                        <nav class="level">'''

                target_file_name = f'{city}-{summary_file_name_prefix}-{date_of_data}.html'
                city_problem_count = 0
                for problem_type, problem_list in problem_lists.items():
                    contents += f'''
                        <div class="level-item has-text-centered">
                            <div>
                              <p class="heading" style="font-size: 20px">{problem_type}</p>
                              <p class="title" style="font-size: 40px"><a href='{target_file_name}#{problem_type}'>
                                {len(problem_list)}</a></p>
                            </div>
                        </div>
                        '''
                    city_problem_count += len(problem_list)
                contents_aside += f'''<li><a href="#{city}">{city}({city_problem_count})</a></li>'''
                contents += '</nav></div>'
            contents_aside += f'</ul></li>'  # 封闭aside的片区目录
        # contents += '</div></div>' # 封闭hero-body
        contents += f'</section></div>'
        contents_aside += f'</ul></nav></aside>'  # 封闭aside目录
        contents += contents_aside
        contents += f'</div>'  # 封闭bd-duo
        contents += html_footer
        contents += html_tail
        n.writelines(contents)
        n.close()


def gen_city_page(map_file_lists, summary_file_name_prefix, city_in_chinese, output_folder):
    """

    :param city_in_chinese:
    :param summary_file_name:
    :param map_file_lists:  字典变量，内含列表变量，{问题类型: [地市名称, 问题类型, 问题排序, 涉及小区, 严重程度, 文件链接]}
    :return:
    """
    summary_file_name = f'{city_in_chinese}-{summary_file_name_prefix}-{date_of_data}.html'
    province_file_name = f'全省-{summary_file_name_prefix}-{date_of_data}.html'
    summary_file = os.path.join(output_folder, sub_folder, summary_file_name)

    html_header = f'''
        <!DOCTYPE html>
        <html>
          <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>{title}--{city_in_chinese}</title>
            <link rel="stylesheet" href="resources/bulma.min.css">
            <link rel="stylesheet" href="resources/bulma-docs.min.css">
            <script defer src="resources/all.js"></script>
          </head>
          <body>
          
    '''
    html_footer = f'''
        <footer class="footer">
          <div class="content has-text-centered">
            <p>
              该网页CSS使用 <a href="https://jgthms.com">Jeremy Thomas</a>编写的<strong>Bulma</strong> . 
                其源代码使用<a href="http://opensource.org/licenses/mit-license.php">MIT</a>许可证. 
            </p>
          </div>
        </footer>
    '''
    html_tail = f'''
        </body>
    </html>
    '''
    with open(summary_file, 'w') as n:
        n.writelines(html_header)
        contents = f'''<div class="bd-duo"><div class="bd-lead">'''
        contents += f'''
            <section id="{city_in_chinese}" class="section">
                <div class="container">
                  <h1 class="title has-text-centered">
                    {title}--{city_in_chinese}
                  </h1>
                  <p class="subtitle has-text-centered">
                    {subtitle} 
                  </p>
                </div>
                <div class="notification is-success is-light has-text-centered">
                {notice}
                </div>
            </section>
            '''
        problem_counts = {}
        problem_type_list = []
        for problem_type, map_file_list in map_file_lists.items():
            problem_counts[problem_type] = len(map_file_list)
            problem_type_list.append(problem_type)
            if problem_counts[problem_type] > 0:
                contents += f'<section id="{problem_type}" class="section">'
                contents += f'''
                    <div class="tile is-parent">
                            <article class="message is-link tile is-child">
                                <div class="message-header">{problem_type}判断条件</div>
                                    <div div class="message-body">
                    '''
                if problem_type == TYPE_PCI:
                    contents += f'''
                        <li>小区1自身覆盖面积大于<code>{pci_a_area}</code>平方米</li>
                        <li>小区2自身覆盖面积大于<code>{pci_b_area}</code>平方米</li>
                        <li>冲突小区覆盖重叠面积大于<code>{pci_st_area}</code>平方米</li>
                        <li>同时覆盖这两个同PCI小区的受影响小区数量大于<code>{pci_neighbor_count}</code>个</li>
                        <li>返回严重程度最高的前<code>{pci_return_limit}</code>个相关小区</li>'''
                elif problem_type == TYPE_OVERSHOOT:
                    contents += f'''
                        <li>小区自身覆盖面积大于<code>{os_a_area}</code>平方米</li>
                        <li>该小区与超过<code>N</code>个覆盖面积大于<code>{os_b_area}</code>平方米的
                        <span class="has-text-danger-dark">同频</span>相邻小区有重叠覆盖
                        且这些重叠部分面积均超过<code>{os_st_area}</code>平方米</li> 
                        <li>在深圳、广州、东莞、佛山<code>N={os_nei_count_thresh_1}</code>，
                        在江门、珠海、中山、惠州<code>N={os_nei_count_thresh_2}</code>，
                        在其余地市<code>N={os_nei_count_thresh_3}</code></li>
                        <li>仅计算<code>{str.replace(str(os_b_arfcn)[1:-1], "'", "")}</code>频点小区</li>
                        <li>返回严重程度最高的前<code>{os_return_limit}</code>个结果</li>'''
                elif problem_type == TYPE_MOD30:
                    contents += f''
                elif problem_type == TYPE_OVERLAP:
                    contents += f'''
                        <li>小区1自身覆盖面积均大于<code>{ol_a_area}</code>平方米</li>
                        <li>小区2自身覆盖面积均大于<code>{ol_b_area}</code>平方米</li>
                        <li>两个小区<code>eNodeB id</code>和<code>频点</code>相同</li>
                        <li>两个小区覆盖重叠面积占各自面积比例均大于<code>{int(ol_st_area_percent * 100)}%</code></li>
                        <li>返回严重程度最高的前<code>{ol_return_limit}</code>个相关小区</li>
                        '''
                else:
                    pass
                contents += f'</div></article></div>'  # 封闭判断条件div
                contents += f'''       
                        <table class="table is-bordered is-striped is-narrow is-hoverable is-fullwidth">
                            <thead>
                                <tr>
                                    <th>地市</th>
                                    <th>问题类型</th>
                                    <th>问题排序</th>
                                    <th>涉及小区</th>
                                    <th>影响程度</th>
                                    <th>文件链接</th>
                                </tr>
                            </thead>
                            <tbody>
                    '''
                for (city, type, priority, cells, impact, in_file) in map_file_list:
                    city_in_chinese = convert_city_name(city)
                    if problem_type == TYPE_PCI:
                        impact_desc = f'影响{impact}个小区'
                    elif problem_type == TYPE_OVERSHOOT:
                        impact_desc = f'{impact}个同频邻区'
                    elif problem_type == TYPE_MOD30:
                        impact_desc = f'{impact}平方米重叠面积'
                    elif problem_type == TYPE_OVERLAP:
                        impact_desc = f'重叠比例{impact}%'
                    else:
                        impact_desc = f'{impact}'
                    contents += f'''
                            <tr>
                                <td>{city_in_chinese}</td>
                                <td>{problem_type}</td>
                                <td>{priority}</td>
                                <td>{cells}</td>
                                <td>{impact_desc}</td>
                                <td><a class='button is-primary' href='{in_file}'>查看</a></td>
                            </tr>
                        '''

                    # replace_local_resouces(in_file=os.path.join(output_folder, sub_folder, in_file))
                contents += '</tbody></table></section>'
        contents += f'</div>'  # for <div class="bd-lead">
        contents += f'''
        <aside class="bd-side">
            <nav id="anchors" class="bd-anchors is-active bd-content is-pinned">
                <ul class="bd-anchors-list content">
                <li><a href="#{city_in_chinese}">{city_in_chinese}</a></li>
                <li><ul>
                
            '''
        for problem_type in problem_type_list:
            contents += f'''
                <li><a href="#{problem_type}">{problem_type}({problem_counts[problem_type]})</a></li>
                '''
        contents += f'</ul><li><a href="{province_file_name}#{city_in_chinese}">回到全省</a></li>'
        contents += f'</li></ul></nav></aside>'  # 封闭aside边缘目录
        contents += f'</div>'  # for <div class="bd-duo">
        n.writelines(contents)
        n.writelines(html_footer)
        n.writelines(html_tail)
        n.close()
    return True


def convert_city_name(name_in_py):
    name_in_py = str.upper(name_in_py)
    if name_in_py == 'SZ':
        return '深圳'
    elif name_in_py == 'GZ':
        return '广州'
    elif name_in_py == 'DG':
        return '东莞'
    elif name_in_py == 'FS':
        return '佛山'
    elif name_in_py == 'HZ':
        return '惠州'
    elif name_in_py == 'ZS':
        return '中山'
    elif name_in_py == 'JM':
        return '江门'
    elif name_in_py == 'ZH':
        return '珠海'
    elif name_in_py == 'HY':
        return '河源'
    elif name_in_py == 'JY':
        return '揭阳'
    elif name_in_py == 'MM':
        return '茂名'
    elif name_in_py == 'QY':
        return '清远'
    elif name_in_py == 'SG':
        return '韶关'
    elif name_in_py == 'ST':
        return '汕头'
    elif name_in_py == 'SW':
        return '汕尾'
    elif name_in_py == 'YF':
        return '云浮'
    elif name_in_py == 'YJ':
        return '阳江'
    elif name_in_py == 'ZJ':
        return '湛江'
    elif name_in_py == 'ZQ':
        return '肇庆'
    elif name_in_py == 'CZ':
        return '潮州'
    elif name_in_py == 'MZ':
        return '梅州'
    else:
        return name_in_py


def load_from_json():
    """
    从已有的json文件中恢复之前生成的问题清单
    :return:
    """
    my_json_file = os.path.join(output_folder, sub_folder, json_file_name)
    if os.path.exists(my_json_file):
        with open(my_json_file, 'r') as json_file:
            json_str = '\n'.join(json_file.readlines())
        problem_summary = json.loads(json_str)
        if problem_summary:
            return problem_summary
        else:
            return False
    else:
        return False


def save_to_json(problem_summary):
    if problem_summary:
        json_str = json.dumps(problem_summary, indent=4, ensure_ascii=False)
        my_json_file = os.path.join(output_folder, sub_folder, json_file_name)
        with open(my_json_file, 'w') as json_file:
            json_file.write(json_str)
        return True
    else:
        return False


if __name__ == '__main__':
    problem_summary = load_from_json()
    if problem_summary:
        print(f'已有{problem_summary.keys()}的记录')
    else:
        print(f'未发现记录文件，重新生成')
        problem_summary = {}
    for area, area_cities in cities.items():
        for city in area_cities:
            city_in_chinese = convert_city_name(city)
            if city not in problem_summary.keys():
                return_lists = {}
                return_lists[TYPE_OVERSHOOT] = gen_over_shooting_cells([city], output_folder=output_folder,
                                                                       map_tiles=map_types[current_map_type])
                return_lists[TYPE_PCI] = gen_pci_conflict_cells([city], output_folder=output_folder,
                                                                map_tiles=map_types[current_map_type])
                return_lists[TYPE_OVERLAP] = gen_overlap_cells([city], output_folder=output_folder,
                                                               map_tiles=map_types[current_map_type])
                # return_lists[TYPE_MOD30] = gen_mod30_cells([city], output_folder=output_folder,
                # map_tiles=map_tiles_gaode)

                problem_summary[city_in_chinese] = return_lists
            else:
                return_lists = problem_summary[city]
            gen_city_page(return_lists, summary_file_name_prefix='精准RF优化问题小区清单',
                          city_in_chinese=city_in_chinese, output_folder=output_folder)
    save_to_json(problem_summary=problem_summary)
    gen_province_page(problem_summary, summary_file_name_prefix='精准RF优化问题小区清单', output_folder=output_folder)
