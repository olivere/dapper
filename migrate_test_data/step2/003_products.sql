# Products table
CREATE TABLE products (
  id integer not null primary key autoincrement,
  sku text,
  name text,
  category_id integer
);

-- Create categories table
CREATE TABLE categories (
  id integer not null primary key autoincrement,
  parent_id integer,
  name text
);
