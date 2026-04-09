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
                 NVL(b.vlpis,0) AS VLPIS,
                 NVL(b.vlcofins,0) AS VLCOFINS,
                 b.codfiscal
          FROM   pcnfent n , pcfornec f  ,  pcnfbaseent b
         WHERE n.codfornec = f.codfornec(+)
           AND b.numtransent = n.numtransent
           AND n.codfilialnf = '{codfilial}'
           AND TRUNC(n.dtent) BETWEEN TO_DATE('{data_inicio}', 'DD/MM/YYYY') AND TO_DATE('{data_fim}', 'DD/MM/YYYY')
           AND n.especie = 'NF'
           AND b.especie = 'NF'
           AND B.codfiscal IN (1556,2556)
           AND NVL(n.conferido, 'N') = 'N'
           AND n.dtcancel is null
          ORDER BY n.codfilial , n.numnota;
      
    
