SELECT 
    up.state,
    COUNT(*) AS total_sales
FROM 
    `de07-oleksandr-anosov.gold.user_profiles_enriched` AS up
JOIN 
    `de07-oleksandr-anosov.silver.sales` AS s
ON 
    up.client_id = s.client_id
WHERE 
    s.product_name = 'TV'
    AND DATE_DIFF(CURRENT_DATE(), up.birth_date, YEAR) BETWEEN 20 AND 30
    AND s.purchase_date BETWEEN '2022-09-01' AND '2022-09-10'
GROUP BY 
    up.state
ORDER BY 
    total_sales DESC
LIMIT 1;