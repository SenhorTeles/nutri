SELECT   n.codfilialnf,
                 n.codfilial,
                 n.modelo,
                 n.serie,
                 n.especie,
                 n.numnota,
                 n.numtransent,
                 n.vltotal, 
                 n.codfornec,
                 f.fornecedor, 
                 f.cgc,
                 n.chavenfe,
                 n.chavecte,
                 n.dtemissao,
                 n.conferido,
                 n.dtent,
                 NVL(b.vlicms, 0) AS vlicms,
                 --- Incluido PIS e COFINS
                 NVL(e.vlpis,0) AS VLPIS,
                 NVL(e.vlcofins,0) AS VLCOFINS,
                 B.codfiscal
          FROM   pcnfent n , pcfornec f  ,  pcnfbase b, pcnfentpiscofins e
         WHERE n.codfornec = f.codfornec(+)
           AND b.numtransent = n.numtransent
           and b.numtransent = e.numtransent(+)
           and n.numtransent = e.numtransent(+)
           AND n.codfilialnf = '{codfilial}'
           AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY')
           AND n.especie = 'NS'
           AND B.codfiscal IN (1933,2933)
           AND NVL(n.conferido, 'N') = 'N'
           AND n.dtcancel is null
           
          ORDER BY n.codfilial , n.numnota;
         
