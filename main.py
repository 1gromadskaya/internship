import pandas as pd


def load_data(file_path):
    return pd.read_excel(file_path)


def aggregate_data(df):
    group_cols = [
        'year',
        'plant_id',
        'produced_material',
        'produced_material_production_type',
        'produced_material_release_type',
        'component_material',
        'component_material_production_type',
        'component_material_release_type'
    ]
    return df.groupby(group_cols, dropna=False)[
        ['produced_material_quantity', 'component_material_quantity']
    ].sum().reset_index()


def explode_hierarchy(df_annual):
    exploded_rows = []
    context_groups = df_annual.groupby(['plant_id', 'year'])

    for (plant, year), group in context_groups:
        bom_map = {mat: group[group['produced_material'] == mat] for mat in group['produced_material'].unique()}
        fin_materials = group[group['produced_material_release_type'] == 'FIN']

        for _, fin_row in fin_materials.iterrows():
            fin_id = fin_row['produced_material']

            if fin_id in bom_map:
                first_level_children = bom_map[fin_id]
                for _, child_row in first_level_children.iterrows():
                    child_id = child_row['component_material']
                    stack = [child_id]

                    while stack:
                        curr_parent_id = stack.pop()

                        if curr_parent_id in bom_map:
                            components = bom_map[curr_parent_id]
                            for _, comp_row in components.iterrows():
                                new_row = {
                                    'plant': plant,
                                    'fin_material_id': fin_id,
                                    'fin_material_release_type': fin_row['produced_material_release_type'],
                                    'fin_material_production_type': fin_row['produced_material_production_type'],
                                    'fin_production_quantity': fin_row['produced_material_quantity'],
                                    'prod_material_id': comp_row['produced_material'],
                                    'prod_material_release_type': comp_row['produced_material_release_type'],
                                    'prod_material_production_type': comp_row['produced_material_production_type'],
                                    'prod_material_production_quantity': comp_row['produced_material_quantity'],
                                    'component_id': comp_row['component_material'],
                                    'component_material_release_type': comp_row['component_material_release_type'],
                                    'component_material_production_type': comp_row[
                                        'component_material_production_type'],
                                    'component_consumption_quantity': comp_row['component_material_quantity'],
                                    'year': year
                                }
                                exploded_rows.append(new_row)

                                next_child_id = comp_row['component_material']
                                if next_child_id != curr_parent_id:
                                    stack.append(next_child_id)

    return pd.DataFrame(exploded_rows)


def format_output(df):
    final_cols = [
        'plant',
        'fin_material_id', 'fin_material_release_type', 'fin_material_production_type', 'fin_production_quantity',
        'prod_material_id', 'prod_material_release_type', 'prod_material_production_type',
        'prod_material_production_quantity',
        'component_id', 'component_material_release_type', 'component_material_production_type',
        'component_consumption_quantity',
        'year'
    ]
    if df.empty:
        return pd.DataFrame(columns=final_cols)
    return df[final_cols]


def main():
    file_name = 'task_2_data_ex.xlsx'
    output_file = 'result.xlsx'

    try:
        df = load_data(file_name)
        df_annual = aggregate_data(df)
        df_exploded = explode_hierarchy(df_annual)
        final_df = format_output(df_exploded)
        final_df.to_excel(output_file, index=False)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()