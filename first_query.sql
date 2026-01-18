select count(film_category.film_id) AS number_of_films, category.name AS name_of_category
from category
JOIN film_category ON film_category.category_id = category.category_id
group by category.name
ORDER BY number_of_films DESC