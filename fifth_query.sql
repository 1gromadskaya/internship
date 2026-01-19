SELECT CONCAT(actor.first_name, ' ', actor.last_name) AS full_name, COUNT(film_actor.actor_id) AS films_count FROM actor
JOIN film_actor ON film_actor.actor_id = actor.actor_id
JOIN film_category ON film_category.film_id = film_actor.film_id
JOIN category ON category.category_id = film_category.category_id
WHERE category.name = 'Children'
GROUP BY actor.actor_id, actor.first_name, actor.last_name
ORDER BY films_count DESC
LIMIT 3