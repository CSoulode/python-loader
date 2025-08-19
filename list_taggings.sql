-- List all taggings
-- This query selects all taggings (with their value and tagset) grouped by media and 
SELECT
    t.*,
    COALESCE(ant.name, tst.name::text, tt.name::text, dt.name::text, nt.name::text) AS tag_name,
    ts.name AS tagset_name
FROM
    public.taggings t
JOIN public.tags tg ON t.tag_id = tg.id
JOIN public.tagsets ts ON tg.tagset_id = ts.id
LEFT JOIN public.alphanumerical_tags ant ON t.tag_id = ant.id
LEFT JOIN public.timestamp_tags tst ON t.tag_id = tst.id
LEFT JOIN public.time_tags tt ON t.tag_id = tt.id
LEFT JOIN public.date_tags dt ON t.tag_id = dt.id
LEFT JOIN public.numerical_tags nt ON t.tag_id = nt.id
ORDER BY t.object_id ASC, t.tag_id ASC;
