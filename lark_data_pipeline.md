



```c++

=arrayformula(let(
    /* =============== PARAMS ================ */
    raw_src, raw_person_address!A:Z,



    /* ================== MODULES ============ */

    trim_table, lambda(tbl, 
        /* ここに1列目のすべての行を見て、空白ではない行のみ抽出する モジュールを書く*/
        tbl
    ),


    header_of, lambda(tbl, take(tbl, 1)),
    body_of, lambda(tbl, drop(tbl, 1)),
    select, lambda(tbl, cols,
        let(
            hdr, header_of(tbl),
            choosecols(xmatch(cols, hdr))
        )
    ),

    /* ============= PIPELINE ============== */
    src, trim_table(raw_src),
    select(src, {"addressline1", "addressid"})



    /* ============== OUTPUT ================== */
    select(src, {"addressline1", "addressid"})
))



=arrayformula(let(
    raw_src, raw_person_address!A:Z,
    trim_table, lambda(tbl, 
        tbl
    ),
    header_of, lambda(tbl, take(tbl, 1)),
    body_of, lambda(tbl, drop(tbl, 1)),
    select, lambda(tbl, cols,
        let(
            hdr, header_of(tbl),
            choosecols(tbl, xmatch(cols, hdr))
        )
    ),

    src, trim_table(raw_src),
    select(src, {"addressline1", "addressid"})
))

```