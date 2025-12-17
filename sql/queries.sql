USE student_rooms;

SELECT
    r.id,
    r.name AS room_name,
    COUNT(s.id) AS student_count
FROM rooms r
LEFT JOIN students s ON r.id = s.room
GROUP BY r.id, r.name
ORDER BY student_count DESC;

SELECT
    r.id,
    r.name AS room_name,
    ROUND(AVG(YEAR(CURDATE()) - YEAR(s.birthday)), 2) AS avg_age
FROM rooms r
JOIN students s ON r.id = s.room
GROUP BY r.id, r.name
ORDER BY avg_age ASC
LIMIT 5;

SELECT
    r.id,
    r.name,
    MAX(YEAR(CURDATE()) - YEAR(s.birthday)) -
    MIN(YEAR(CURDATE()) - YEAR(s.birthday)) AS age_diff,
    COUNT(s.id) AS student_count
FROM rooms r
JOIN students s ON r.id = s.room
GROUP BY r.id, r.name
HAVING student_count >= 2
ORDER BY age_diff DESC
LIMIT 5;

SELECT
    r.id,
    r.name
FROM rooms r
WHERE EXISTS (
    SELECT 1 FROM students s1
    WHERE s1.room = r.id AND s1.sex = 'M'
) AND EXISTS (
    SELECT 1 FROM students s2
    WHERE s2.room = r.id AND s2.sex = 'F'
);
