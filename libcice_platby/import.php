<?php
class import extends Main
{

    function load($file)
    {
        $data = fopen("$file", r);
        $radek = 0;
        echo "<h1>Stav importu</h1>";
        echo "<div id=\"datawindow\">";
        while ($platby = fgetcsv($data, filesize("$file"), ";", '"')) {
            $pocet = count($platby);
            if ($pocet == 23) {
            	$chyba = false;
                if ($radek == 0) {
                    echo "Zakládám nový import<br/>";
                    $sql = $this->prp("INSERT INTO import VALUES (NULL,1,NOW(),0)");
                    $sql->execute();
                    $id = $this->lastId(import);
                    echo "Import založen číslo importu $id<br/>";
                } else {
                    foreach ($platby as $index => $info) {
                        //echo $info;
                        $platby[$index] = iconv("windows-1250", "utf-8", $info);
                    }
                    echo "probíhá import řádku $radek";
                    $sql3 = $this->prp("SELECT idprikazy FROM prikazy WHERE bankovni_reference=?");
                    $sql3->execute(array($platby[9]));
                    $this->sqlchyba($sql3);
					$data3 = $sql3->fetch();
                    if ($data3[0])
                    {
                    	echo " Existující bankovní reference řádek nebude importován<br/>";
                    	header("Location: index.php");
                    	die("Import zrušen");
                    	
                    }
                    $sql = $this->prp("INSERT INTO prikazy VALUES (NULL,$id,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)");
                    $platby[5] = str_replace(array(",", " "), array(".", ""), $platby[5]);
                    $pd = explode(".",$platby[3]);
                    $pd2 = explode(".",$platby[4]);
                    $platby[3] = $pd[2]."-".$pd[1]."-".$pd[0];
                    $platby[4] = $pd2[2]."-".$pd2[1]."-".$pd2[0];
                    //print_r($platby);
                    $ch = 0;
                    $sql->execute($platby);
                    $ch = $this->sqlchyba($sql);
                    if ($ch > 0)
                        echo "..Chyba<br/>";
                    else
                        echo "..Ok<br/>";
                }
                $radek++;

            } else {
            	$chyba = true;
                echo "Import nemůže proběhnout, počet sloupcu neodpovídá";
            }


        }
        if ($chyba == false)
        {
        	echo "Probíhá párování pladeb podle variabilního symbolu<br/>";
        	
        	$clo = $this->prp("SELECT idnody,idlide,vs,nick FROM lide WHERE vs=?");
        	$vp = $this->prp("INSERT INTO lide_platby VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
        	$up = $this->prp("UPDATE prikazy SET pouzito=1 WHERE idprikazy=?");
        	$de = $this->prp("DELETE FROM prikazy WHERE idprikazy=?");
        	
        	$sql = $this->prp("SELECT idimport,castka,variabilni_symbol,nazev_protiuctu,cislo_protiuctu,banka_protiuctu,splatnost,idprikazy FROM prikazy WHERE pouzito=0 AND typ_transakce NOT IN (SELECT nazev FROM transakce)");
        	$sql->execute();
        	$data = $sql->fetchAll();
        	$this->sqlchyba($sql);
        	foreach ($data AS $info)
        	{
		    $varFull = str_pad($info['variabilni_symbol'],10,"0",STR_PAD_LEFT);
        		echo "Platba $info[idprikazy] ($varFull) ";
        		$clo->execute(array($varFull));
				$cdata = $clo->fetch();
				$this->sqlchyba($clo);	
				if ($cdata[idlide])
				{
					echo "páruji platbu s $cdata[nick]";
					$vp->execute(array("$cdata[idlide]","$cdata[idnody]",4,$info[idprikazy],$info[idimport],$info[splatnost],$info[splatnost],null,$info[nazev_protiuctu],$info[cislo_protiuctu],$info[banka_protiuctu],$cdata[vs]));
					$dc = $this->sqlchyba($vp);
					if ($dc > 0 AND $dc<>1062)
					echo "..Chyba<br/>";
					elseif ($dc==1062)
					{
						echo "...již spárováno (duplikát)- mažu příkaz</br>";
						$de->execute(array($info[idprikazy]));
					}
					else
					{
					echo "..Ok<br/>";
					$up->execute(array($info[idprikazy]));
					}
				}
				else
				{
					echo "...nenalezen<br/>";
				}
        	}
        	
			echo "Probíhá párování pladeb podle typu transakce</br>";
			$typ = $this->prp("SELECT idtransakce FROM transakce WHERE nazev=?");
			$vlt = $this->prp("INSERT INTO prikazy_transakce VALUES (?,?,?,?,?)");
			
			$sql = $this->prp("SELECT idimport,castka,splatnost,idprikazy,typ_transakce FROM prikazy WHERE pouzito=0 AND typ_transakce IN (SELECT nazev FROM transakce)");
        	$sql->execute();
        	$data = $sql->fetchAll();
        	$this->sqlchyba($sql);
        	foreach ($data AS $info)
        	{
        		echo "Platba $info[idprikazy] ($info[typ_transakce]) ";
        		$typ->execute(array("$info[typ_transakce]"));
        		$this->sqlchyba($typ);
        		$md = $typ->fetch();
				$vlt->execute(array($info[idprikazy],$info[idimport],$md[idtransakce],$info[splatnost],$info[castka]));
				$this->sqlchyba($vlt);
				$up->execute(array($info[idprikazy]));
        		echo "..Ok<br/>";
       		}
			
			echo "Import a párování dokončeno";
        	
        	
        	
        	
        }
        
        echo "</div>";
    }

    function form()
    {
    	echo "<div id=\"datawindow\">";
    	echo "Naimportované soubory:<br/>";
    	$d = dir("csv");
    	//print_r($d);
    	while (false !== ($entry = $d->read())) {
   if ($entry != "." && $entry != "..") {
   echo $entry."<br/>";
   }
}
$d->close();
    	echo "</div>";
        echo "<h2>Upozornění: Systém ihned po importu provádí párování, taktéž se pokouší spárovat platby, které došli později od předchozího importu nebo se snaží spárovat lidi, kteří již byli přidáni ale zaplatily dřív</h2>";
		echo "<form action=\"index.php?menu=import&amp;l=true\" enctype=\"multipart/form-data\" method=\"post\">";
        echo "<fieldset><legend>Zvolte soubor pro ipmport</legend>";
        echo "<label>Soubor:</label><input type=\"file\" name=\"soubor\"/>";
        $this->button("Nahrát na server");
        echo "</fieldset>";
        echo "</form>";
	
	
	echo "<div>";
	echo "<h2>Vyberte číslo importu pro odmazání</h2>";
	
	$s00 = $this->prp("SELECT idimport,datum FROM import ORDER BY datum DESC");
	$s00->execute();
	$import = $s00->fetchAll();
	$this->sqlchyba($s00);
	
	echo "<form action=\"index.php?menu=import&amp;sm=true\" enctype=\"multipart/form-data\" method=\"post\">";
	echo "<fieldset><legend>Zvolte import pro výmaz</legend>";
	echo '<select name="idimport">';
	print_R($import);
	foreach ($import AS $info)
	{
	    echo "<option value=\"$info[0]\">$info[1]</option>";
	
	}
	echo '</select>';
	$this->button("Smazat ze serveru");
	echo "</fieldset>";
	echo "</form>";
	echo "</div>";
	
	
    }
    
    
    function SmazatImport()
    {
	$id = $_POST[idimport];
	
	$sql = $this->prp("DELETE FROM lide_platby WHERE idimport=?");
	$sql->execute(array($id));
	$this->sqlchyba($sql);
	
	
	$sql = $this->prp("DELETE FROM prikazy WHERE idimport=?");
	$sql->execute(array($id));
	$this->sqlchyba($sql);
	
	$sql = $this->prp("DELETE FROM import WHERE idimport=?");
	$sql->execute(array($id));
	$this->sqlchyba($sql);


	echo "<h1>Import smazán</h1>";

	
    }
    
    function loadFile()
    {
    	$soubor = $_FILES[soubor];
    	$typ = $soubor[type];
     	$nazev = $soubor[name];
     	$konec = substr($nazev,-3);
     	
    
    	if ($soubor[error]<>0)
    	throw new Exception("Chyba při nahrávání na server",10163);
    	if ($konec!='csv')
    	throw new Exception ("Soubor není CSV",10165);
    	if (!$e)
    	{
    		if (@move_uploaded_file($soubor[tmp_name],"csv/$soubor[name]"))
    		{
    			echo "Soubor byl nahrán";
    			chmod("csv/$soubor[name]",0777);
    			$this->load("csv/$soubor[name]");
   			}
    		else
    		throw new Exception("Chyba při přesunud do složky",10171);
    	}
    }


}


?>
