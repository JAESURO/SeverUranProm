SELECT * FROM powerplants LIMIT 10;

SELECT * FROM uranium_companies LIMIT 10;

SELECT * FROM uranium_mines LIMIT 10;

SELECT name_of_powerplant, capacity_mw FROM powerplants WHERE capacity_mw > 2000 ORDER BY capacity_mw DESC;

SELECT company, tonnes_u FROM uranium_companies WHERE tonnes_u > 5000 ORDER BY tonnes_u DESC;

SELECT name_of_powerplant, capacity_mw FROM powerplants WHERE primary_fuel = 'Nuclear' ORDER BY capacity_mw DESC;

SELECT country_long, COUNT(*) FROM powerplants GROUP BY country_long;

SELECT country_long, SUM(tonnes_u) FROM uranium_companies GROUP BY country_long;

SELECT m.mine_name, m.country_long, c.company FROM uranium_mines m JOIN uranium_companies c ON m.country_long = c.country_long LIMIT 10;

SELECT m.country_long, COUNT(*) as mine_count, r.tonnes_uranium FROM uranium_mines m JOIN uranium_reserves r ON m.country_long = r.country_long GROUP BY m.country_long, r.tonnes_uranium ORDER BY r.tonnes_uranium DESC LIMIT 10;