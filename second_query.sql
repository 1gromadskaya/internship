SELECT CONCAT(first_name, ' ', last_name) AS full_name, film.rental_rate FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
JOIN film ON film.film_id = film_actor.film_id
ORDER BY rental_rate DESC
LIMIT 10