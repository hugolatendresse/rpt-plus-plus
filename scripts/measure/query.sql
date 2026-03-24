SET max_temp_directory_size='0KiB';
SET threads = 1;
SET disabled_optimizers = 'compressed_materialization';

SELECT min(b.valueB1)
FROM a
JOIN b ON a.keyB1 = b.keyB1;
