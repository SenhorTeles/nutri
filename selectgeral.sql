🔹 CTE
SELECT 
    n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, 
    n.numnota, n.numtransent, n.vltotal, n.codfornec, 
    f.fornecedor, f.cgc, n.chavenfe, n.chavecte, 
    n.dtemissao, n.conferido, n.dtent, 
    NVL(b.vlicms, 0) AS vlicms, 
    NVL(b.vlpis,0) AS vlpis, 
    NVL(b.vlcofins,0) AS vlcofins, 
    b.codfiscal 
FROM 
    pcnfent n, pcfornec f, pcnfbaseent b 
WHERE 
    n.codfornec = f.codfornec(+) 
    AND b.numtransent = n.numtransent 
    AND n.codfilialnf = '{codfilial}' 
    AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') 
                           AND TO_DATE('{data_fim}', 'DD/MM/YYYY') 
    AND n.especie = 'CT' 
    AND b.especie = 'CT' 
    AND NVL(n.conferido, 'N') = 'N' 
    AND n.dtcancel IS NULL 
ORDER BY n.codfilial, n.numnota;


🔹 DANFE
SELECT 
    n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, 
    n.numnota, n.numtransent, n.vltotal, n.codfornec, 
    f.fornecedor, f.cgc, n.chavenfe, n.chavecte, 
    n.dtemissao, n.conferido, n.dtent, 
    NVL(b.vlicms, 0) AS vlicms, 
    NVL(b.vlpis,0) AS vlpis, 
    NVL(b.vlcofins,0) AS vlcofins, 
    b.codfiscal 
FROM 
    pcnfent n, pcfornec f, pcnfbaseent b 
WHERE 
    n.codfornec = f.codfornec(+) 
    AND b.numtransent = n.numtransent 
    AND n.codfilialnf = '{codfilial}' 
    AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') 
                           AND TO_DATE('{data_fim}', 'DD/MM/YYYY') 
    AND n.especie = 'NF' 
    AND b.especie = 'NF' 
    AND b.codfiscal NOT IN (1556,2556) 
    AND NVL(n.conferido, 'N') = 'N' 
    AND n.dtcancel IS NULL 
ORDER BY n.codfilial, n.numnota;


🔹 CONSUMO
SELECT 
    n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, 
    n.numnota, n.numtransent, n.vltotal, n.codfornec, 
    f.fornecedor, f.cgc, n.chavenfe, n.chavecte, 
    n.dtemissao, n.conferido, n.dtent, 
    NVL(b.vlicms, 0) AS vlicms, 
    NVL(b.vlpis,0) AS vlpis, 
    NVL(b.vlcofins,0) AS vlcofins, 
    b.codfiscal 
FROM 
    pcnfent n, pcfornec f, pcnfbaseent b 
WHERE 
    n.codfornec = f.codfornec(+) 
    AND b.numtransent = n.numtransent 
    AND n.codfilialnf = '{codfilial}' 
    AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') 
                           AND TO_DATE('{data_fim}', 'DD/MM/YYYY') 
    AND n.especie = 'NF' 
    AND b.especie = 'NF' 
    AND b.codfiscal IN (1556,2556) 
    AND NVL(n.conferido, 'N') = 'N' 
    AND n.dtcancel IS NULL 
ORDER BY n.codfilial, n.numnota;


🔹 SERVIÇO
SELECT 
    n.codfilialnf, n.codfilial, n.modelo, n.serie, n.especie, 
    n.numnota, n.numtransent, n.vltotal, n.codfornec, 
    f.fornecedor, f.cgc, n.chavenfe, n.chavecte, 
    n.dtemissao, n.conferido, n.dtent, 
    NVL(b.vlicms, 0) AS vlicms, 
    NVL(e.vlpis,0) AS vlpis, 
    NVL(e.vlcofins,0) AS vlcofins, 
    b.codfiscal 
FROM 
    pcnfent n, pcfornec f, pcnfbase b, pcnfentpiscofins e 
WHERE 
    n.codfornec = f.codfornec(+) 
    AND b.numtransent = n.numtransent 
    AND b.numtransent = e.numtransent 
    AND n.numtransent = e.numtransent 
    AND n.codfilialnf = '{codfilial}' 
    AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') 
                           AND TO_DATE('{data_fim}', 'DD/MM/YYYY') 
    AND n.especie = 'NS' 
    AND b.codfiscal IN (1933,2933) 
    AND NVL(n.conferido, 'N') = 'N' 
    AND n.dtcancel IS NULL 
ORDER BY n.codfilial, n.numnota;