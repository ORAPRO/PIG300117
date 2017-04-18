M = LOAD '/home/edureka/SAIWS/Dataset/PIG/MapData.txt' USING PigStorage(',') AS (a:map[chararray],b:map[int]);
F = FOREACH M generate a#'NAME', b#'AGE';                                     
DUMP F;
