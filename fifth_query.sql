WITH actor_stats AS (
    SELECT 
        CONCAT(a.first_name, ' ', a.last_name) AS full_name, 
        COUNT(fa.film_id) AS films_count,
        DENSE_RANK() OVER (ORDER BY COUNT(fa.film_id) DESC) as rank
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE c.name = 'Children'
    GROUP BY a.actor_id, a.first_name, a.last_name
)
SELECT full_name, films_count
FROM actor_stats
WHERE rank <= 3;