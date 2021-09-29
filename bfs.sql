DROP TABLE IF EXISTS frontier;
DROP TABLE IF EXISTS next;
DROP TABLE IF EXISTS seen;

CREATE TABLE frontier(id INTEGER);
CREATE TABLE next(id INTEGER);
CREATE TABLE seen(id INTEGER, level INTEGER);

INSERT INTO next VALUES (1);

INSERT INTO seen (SELECT id, 0 FROM next);
DELETE FROM frontier;
INSERT INTO frontier (SELECT * FROM next);
DELETE FROM next;

INSERT INTO next
    (SELECT e.target
    FROM frontier
    JOIN e
      ON e.source = frontier.id
    WHERE NOT EXISTS (SELECT 1 FROM seen WHERE id = e.target));

INSERT INTO seen (SELECT id, 1 FROM next);
DELETE FROM frontier;
INSERT INTO frontier (SELECT * FROM next);
DELETE FROM next;

INSERT INTO next
    (SELECT e.target
    FROM frontier
    JOIN e
      ON e.source = frontier.id
    WHERE NOT EXISTS (SELECT 1 FROM seen WHERE id = e.target));

INSERT INTO seen (SELECT id, 2 FROM next);
DELETE FROM frontier;
INSERT INTO frontier (SELECT * FROM next);
DELETE FROM next;

INSERT INTO next
    (SELECT e.target
    FROM frontier
    JOIN e
      ON e.source = frontier.id
    WHERE NOT EXISTS (SELECT 1 FROM seen WHERE id = e.target));

INSERT INTO seen (SELECT id, 3 FROM next);
DELETE FROM frontier;
INSERT INTO frontier (SELECT * FROM next);
DELETE FROM next;

SELECT * FROM seen;
