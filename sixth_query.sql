SELECT city.city, 
SUM(CASE WHEN customer.active = 1 THEN 1 ELSE 0 END) AS active_users,
SUM(CASE WHEN customer.active = 0 THEN 1 ELSE 0 END) AS inactive_users
FROM city
JOIN address ON city.city_id = address.city_id
JOIN customer ON address.address_id = customer.address_id
GROUP BY city.city
ORDER BY inactive_users DESC