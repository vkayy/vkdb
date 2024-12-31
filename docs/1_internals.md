# Internals

## Database engine

vkdb is built on log-structured merge (LSM) trees. In their simplest form, these have an in-memory layer and a disk layer, paired with a write-ahead log (WAL) for persistence of in-memory changes.

When you instantiate a `vkdb::Database`, all of the prior in-memory information (in-memory layer, metadata, etc.) will be loaded in if the database already exists, and if not, a new one is set up. This persists on disk until you clear it via `vkdb::Database::clear`.

It's best to make all interactions via `vkdb::Database`, or the `vkdb::Table` type via `vkdb::Database::getTable`, unless you just want to play around with vq (more on this later).

Also, one important thing to note is that all database files will be stored in `vkdb::DATABASE_DIRECTORY`; you shouldn't tamper with this directory nor the files in it.

> [!NOTE]
> All database files will be stored in `vkdb::DATABASE_DIRECTORY`; you shouldn't tamper with this directory unless you want to move your databases between machines.

@dot
digraph G {
    rankdir=LR;
    bgcolor="transparent";
    node [style=filled, fontcolor="white", color="white", shape=rectangle, fontname="Roboto", fillcolor="#404040"];
    edge [color="white"];
    
    subgraph cluster_ui {
        label=<<font face="Roboto" color="black">User Interface</font>>;
        color="white";
        bgcolor="#a0a0a0";
        database [label="Database"];
        tables [label="Tables"];
    }
    
    subgraph cluster_memory {
        label=<<font face="Roboto" color="black">Memory Layer</font>>;
        color="white";
        bgcolor="#a0a0a0";
        c0Layer [label="C0 Layer"];
        memtable [label="Memtable"];
        timestampRangeM [label="Timestamp Range"];
        keyRangeM [label="Key Range"];
        lruCache [label="LRU Cache"];
        
        subgraph cluster_metadata {
            label=<<font face="Roboto" color="black">Metadata</font>>;
            color="white";
            bgcolor="#808080";
            sstableMetadata [label="SSTable Metadata"];
            index [label="Index"];
            timestampRangeS [label="Timestamp Range"];
            keyRangeS [label="Key Range"];
            bloomFilter [label="Bloom Filter"];
        }
    }
    
    subgraph cluster_disk {
        label=<<font face="Roboto" color="black">Disk Layer</font>>;
        color="white";
        bgcolor="#a0a0a0";
        ckLayers [label="Ck Layers"];
        ssTables [label="SSTables"];
        writeAheadLog [label="Write-Ahead Log"];
    }
    
    database -> tables;
    tables -> lsmTree;

    lsmTree [label="LSM Tree"];
    
    lsmTree -> lruCache;
    lsmTree -> writeAheadLog;
    lsmTree -> c0Layer;
    lsmTree -> ckLayers;
    
    c0Layer -> memtable;
    memtable -> timestampRangeM;
    memtable -> keyRangeM;
    
    sstableMetadata -> index;
    sstableMetadata -> timestampRangeS;
    sstableMetadata -> keyRangeS;
    sstableMetadata -> bloomFilter;
    
    ckLayers -> ssTables;
    ssTables -> sstableMetadata;
}
@enddot

## Query processing

Lexing is done quite typically, with enumerated token types and line/column number stored for error messages. Initially, I directly executed queries as string streams, but that was a nightmare for robustness.

In terms of parsing, vq has been constructed to have an LL(1) grammar—this meant I could write a straightforward recursive descent parser for the language. This directly converts queries to an abstract syntax tree (AST) with `std::variant`.

Finally, the interpreter makes quick use of the AST via the visitor pattern, built into C++ with `std::variant` (mentioned earlier) and `std::visit`. This ended up making the interpreter (and pretty-printer) very satisfying to write.

@dot
digraph G {
    rankdir=LR;
    bgcolor="transparent";
    node [style=filled, fontcolor="white", color="white", shape=rectangle, fontname="Roboto", fillcolor="#404040"];
    edge [color="white"];
    
    subgraph cluster_input {
        label=<<font face="Roboto" color="black">Input</font>>;
        color="white";
        bgcolor="#a0a0a0";
        sourceCode [label="Source Code"];
    }
    
    subgraph cluster_lexical {
        label=<<font face="Roboto" color="black">Lexical Analysis</font>>;
        color="white";
        bgcolor="#a0a0a0";
        lexer [label="Lexer"];
        tokens [label="Tokens"];
        tokenType [label="Type"];
        lineNo [label="Line Number"];
        columnNo [label="Column Number"];
        lexeme [label="Lexeme"];
        
        tokenType -> tokens [style=dotted];
        lineNo -> tokens [style=dotted];
        columnNo -> tokens [style=dotted];
        lexeme -> tokens [style=dotted];
    }
    
    subgraph cluster_syntactic {
        label=<<font face="Roboto" color="black">Syntactic Analysis</font>>;
        color="white";
        bgcolor="#a0a0a0";
        parser [label="Recursive Descent Parser"];
        ast [label="Abstract Syntax Tree"];
        variant [label="std::variant"];
        
        variant -> ast [style=dotted];
    }
    
    subgraph cluster_semantic {
        label=<<font face="Roboto" color="black">Semantic Analysis</font>>;
        color="white";
        bgcolor="#a0a0a0";
        interpreter [label="Interpreter"];
        visitor [label="Visitor Pattern"];
        stdVisit [label="std::visit"];
        
        visitor -> interpreter [style=dotted];
        stdVisit -> visitor [style=dotted];
    }
    
    subgraph cluster_output {
        label=<<font face="Roboto" color="black">Output</font>>;
        color="white";
        bgcolor="#a0a0a0";
        result [label="Result"];
    }
    
    sourceCode -> lexer;
    lexer -> tokens;
    tokens -> parser;
    parser -> ast;
    ast -> interpreter;
    interpreter -> result;
}
@enddot