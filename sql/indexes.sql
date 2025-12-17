USE student_rooms;

DROP INDEX IF EXISTS idx_students_room ON students;
CREATE INDEX idx_students_room ON students(room);

DROP INDEX IF EXISTS idx_students_birthday ON students;
CREATE INDEX idx_students_birthday ON students(birthday);

DROP INDEX IF EXISTS idx_students_sex ON students;
CREATE INDEX idx_students_sex ON students(sex);