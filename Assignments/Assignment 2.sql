CREATE TABLE query1 AS SELECT g.name,COUNT(m.title) AS moviecount FROM genres g,movies m,hasagenre h WHERE g.genreid = h.genreid AND m.movieid = h.movieid  GROUP BY g.name;

CREATE TABLE query2 AS SELECT g.name,AVG(r.rating) AS rating FROM genres g,ratings r,hasagenre h WHERE g.genreid = h.genreid AND r.movieid = h.movieid GROUP BY g.name;

CREATE TABLE query3 AS SELECT m.title,COUNT(r.rating) AS countofratings FROM movies m,ratings r WHERE m.movieid = r.movieid GROUP BY m.title HAVING COUNT(r.rating)>='10';

CREATE TABLE query4 AS SELECT m.movieid,m.title FROM genres g,movies m,hasagenre h WHERE g.genreid = h.genreid AND m.movieid = h.movieid AND g.name = 'Comedy' GROUP BY m.movieid;

CREATE TABLE query5 AS SELECT m.title,AVG(r.rating) AS average FROM movies m,ratings r,hasagenre h WHERE m.movieid = h.movieid AND r.movieid = h.movieid GROUP BY m.title;

CREATE TABLE query6 AS SELECT AVG(r.rating) AS average FROM ratings r
WHERE r.movieid IN(SELECT h.movieid from hasagenre h,genres g WHERE g.genreid = h.genreid AND g.name IN('Comedy'));

CREATE TABLE query7 AS SELECT AVG(r.rating) AS average FROM ratings r WHERE r.movieid IN (SELECT movieid FROM(SELECT h.movieid FROM hasagenre h,genres g WHERE g.genreid = h.genreid and g.name IN('Romance')) a
WHERE r.movieid IN(SELECT h.movieid FROM hasagenre h,genres g WHERE g.genreid = h.genreid and g.name IN('Comedy')));

CREATE TABLE query8 AS SELECT AVG(r.rating) AS average FROM ratings r WHERE r.movieid IN (SELECT movieid FROM(SELECT h.movieid FROM hasagenre h,genres g WHERE g.genreid = h.genreid and g.name IN('Romance')) a
WHERE r.movieid NOT IN(SELECT h.movieid FROM hasagenre h,genres g WHERE g.genreid = h.genreid and g.name IN('Comedy')));

CREATE TABLE query9 AS SELECT movieid,rating from ratings WHERE userid=:v1;