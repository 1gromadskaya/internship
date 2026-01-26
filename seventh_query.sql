SELECT 
    category.name,
    JUSTIFY_INTERVAL(SUM(rental.return_date - rental.rental_date)) AS total_time
FROM city
  JOIN address ON city.city_id = address.city_id
  JOIN customer ON address.address_id = customer.address_id
  JOIN rental ON customer.customer_id = rental.customer_id
  JOIN inventory ON rental.inventory_id = inventory.inventory_id
  JOIN film_category ON inventory.film_id = film_category.film_id
  JOIN category ON film_category.category_id = category.category_id
WHERE city.city ILIKE 'a%' OR city.city LIKE '%-%'
GROUP BY category.name
ORDER BY total_time DESC
