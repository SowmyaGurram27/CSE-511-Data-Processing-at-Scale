CREATE TABLE users(
	userid INT NOT NULL,
	name TEXT,
	PRIMARY KEY(userid)
);

CREATE TABLE movies(
	movieid INT NOT NULL,
	title TEXT,
	PRIMARY KEY(movieid)
);

CREATE TABLE taginfo(
	tagid INT NOT NULL,
	content TEXT,
	PRIMARY KEY(tagid)
);

CREATE TABLE genres(
	genreid INT NOT NULL,
	name TEXT,
	PRIMARY KEY(genreid)
);

CREATE TABLE ratings(
	userid INT NOT NULL,
	movieid INT NOT NULL,
	rating NUMERIC CHECK(rating <= 5),
	timestamp BIGINT,
	FOREIGN KEY (userid) REFERENCES users,
	FOREIGN KEY (movieid) REFERENCES movies,
	UNIQUE(userid,movieid)
);

CREATE TABLE tags(
	userid INT NOT NULL,
	movieid INT NOT NULL,
	tagid INT NOT NULL,
	timestamp BIGINT,
	FOREIGN KEY (userid) REFERENCES users,
	FOREIGN KEY (movieid) REFERENCES movies,
	FOREIGN KEY (tagid) REFERENCES taginfo
);

CREATE TABLE hasagenre(
	movieid INT NOT NULL,
	genreid INT NOT NULL,
	FOREIGN KEY (movieid) REFERENCES movies,
	FOREIGN KEY (genreid) REFERENCES genres
);
	

