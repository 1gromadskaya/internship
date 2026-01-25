WITH aggregated_bom AS (
    SELECT 
        year,
        plant_id,
        produced_material,
        produced_material_production_type,
        produced_material_release_type,
        produced_material_quantity,
        component_material,
        component_material_production_type,
        component_material_release_type,
        SUM(TRY_CAST(component_material_quantity AS FLOAT)) as component_material_quantity
    FROM 
        dbo.task_2_data_ex
    GROUP BY 
        year, plant_id, produced_material, produced_material_production_type, 
        produced_material_release_type, produced_material_quantity,
        component_material, component_material_production_type, component_material_release_type
),

level_1_heads AS (
    SELECT 
        aggregated_bom.plant_id, 
        aggregated_bom.year,
        aggregated_bom.produced_material as fin_id,
        aggregated_bom.produced_material_release_type as fin_release_type,
        aggregated_bom.produced_material_production_type as fin_production_type,
        TRY_CAST(aggregated_bom.produced_material_quantity AS FLOAT) as fin_qty,
        aggregated_bom.component_material as start_node_id
    FROM aggregated_bom
    WHERE aggregated_bom.produced_material_release_type = 'FIN'
),

bom_explosion AS (
    SELECT 
        level_1_heads.plant_id, 
        level_1_heads.year,
        level_1_heads.fin_id, 
        level_1_heads.fin_release_type, 
        level_1_heads.fin_production_type, 
        level_1_heads.fin_qty,
        aggregated_bom.produced_material as prod_id,
        aggregated_bom.produced_material_release_type as prod_release_type,
        aggregated_bom.produced_material_production_type as prod_production_type,
        TRY_CAST(aggregated_bom.produced_material_quantity AS FLOAT) as prod_qty,
        aggregated_bom.component_material as comp_id,
        aggregated_bom.component_material_release_type as comp_release_type,
        aggregated_bom.component_material_production_type as comp_production_type,
        aggregated_bom.component_material_quantity as comp_qty
    FROM level_1_heads
    JOIN aggregated_bom ON level_1_heads.start_node_id = aggregated_bom.produced_material 
        AND level_1_heads.plant_id = aggregated_bom.plant_id 
        AND level_1_heads.year = aggregated_bom.year

    UNION ALL
    
    SELECT
        bom_explosion.plant_id, 
        bom_explosion.year,
        bom_explosion.fin_id, 
        bom_explosion.fin_release_type, 
        bom_explosion.fin_production_type, 
        bom_explosion.fin_qty,
        aggregated_bom.produced_material,
        aggregated_bom.produced_material_release_type,
        aggregated_bom.produced_material_production_type,
        TRY_CAST(aggregated_bom.produced_material_quantity AS FLOAT), 
        aggregated_bom.component_material,
        aggregated_bom.component_material_release_type,
        aggregated_bom.component_material_production_type,
        aggregated_bom.component_material_quantity
    FROM bom_explosion
    JOIN aggregated_bom ON bom_explosion.comp_id = aggregated_bom.produced_material 
        AND bom_explosion.plant_id = aggregated_bom.plant_id 
        AND bom_explosion.year = aggregated_bom.year
)

SELECT * FROM bom_explosion
ORDER BY plant_id, year, fin_id, prod_id;