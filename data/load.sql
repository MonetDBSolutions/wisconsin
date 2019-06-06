cat data/ONEKTUP.csv | mclient -d wisconsin -s "copy into ONEKTUP from STDIN";
cat data/TENKTUP1.csv | mclient -d wisconsin -s "copy into TENKTUP1 from STDIN";
cat data/TENKTUP2.csv | mclient -d wisconsin -s "copy into TENKTUP2 from STDIN";

