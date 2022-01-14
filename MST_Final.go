package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

var MAXINT int = 1000000
var MY_DEBUG bool = false
var PRINT_NEIGH_LIST bool = false

var PORT string = ":30000"

type yamlConfig struct {
	ID         int    `yaml:"id"`
	Address    string `yaml:"address"`
	Neighbours []struct {
		ID         int    `yaml:"id"`
		Address    string `yaml:"address"`
		EdgeWeight int    `yaml:"edge_weight"`
	} `yaml:"neighbours"`
}

func initAndParseFileNeighbours(filename string) yamlConfig {
	fullpath, _ := filepath.Abs("./" + filename)
	yamlFile, err := ioutil.ReadFile(fullpath)

	if err != nil {
		panic(err)
	}

	var data yamlConfig

	err = yaml.Unmarshal([]byte(yamlFile), &data)
	if err != nil {
		panic(err)
	}

	return data
}
func Log(file *os.File, message string) {
	_, err := file.WriteString(message)
	if err != nil {
		panic(err)
	}
	//long++

}

func printAddress(funcName string, myAddress string, neighAddress string) {
	fmt.Println(funcName, ":    My Addr: ", myAddress, "    contacting: ", neighAddress)
}

//On envoi test vers le voisin qui a le plus petit poid (MWOE)
// Le message test  contient l'id de fragment
func send_Test(nodeAddress string, neighAddress string, id_frag int) {
	if MY_DEBUG {
		printAddress("TEST", nodeAddress, neighAddress)
	}
	outConn, err := net.Dial("tcp", neighAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(nodeAddress + ":Test:" + strconv.Itoa(id_frag)))
	outConn.Close()
}

// C'est la reponse au TEST
// c'est juste un boolean on l'envoi si l'id_frag récu est different de l'id_frag courrant, on accept le test
func send_accept(nodeAddress string, neighAddress string, boolean bool) {
	if MY_DEBUG {
		printAddress("ACCEPT", nodeAddress, neighAddress)
	}
	outConn, err := net.Dial("tcp", neighAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(nodeAddress + ":Accept:" + strconv.FormatBool(boolean)))
	outConn.Close()
}

// Report the MWOE to the root
func send_report(nodeAddress string, neighAddress_to_parent string, MWOE int, list_address string) {
	if MY_DEBUG {
		printAddress("REPORT", nodeAddress, neighAddress_to_parent)
	}
	outConn, err := net.Dial("tcp", neighAddress_to_parent+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	list_address += "/" + nodeAddress
	outConn.Write([]byte(nodeAddress + ":Report:" + strconv.Itoa(MWOE) + ":" + list_address))
	outConn.Close()
}

// Envoyer par le root vers le edge_node to activate union of fragments
func send_Merge(rootAddress string, edge_node string, list_address string) {
	if MY_DEBUG {
		printAddress("MERGE", rootAddress, edge_node)
	}
	outConn, err := net.Dial("tcp", edge_node+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(rootAddress + ":Merge:" + list_address))
	outConn.Close()
}

// Send by the edge_node to the MWOE  to perform the union
func send_CONNECT(nodeAddress string, node_from_other_fragment string, id_node int) {
	if MY_DEBUG {
		printAddress("CONNECT", nodeAddress, node_from_other_fragment)
	}
	outConn, err := net.Dial("tcp", node_from_other_fragment+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(nodeAddress + ":CONNECT:" + strconv.Itoa(id_node)))
	outConn.Close()
}

// Envoyée par le nouveau root pour annoncer le nouveau fragment
func send_New_Fragment(rootAddress string, nodeAddress string, id_frag int) {
	if MY_DEBUG {
		printAddress("NEW FRAG", rootAddress, nodeAddress)
	}
	outConn, err := net.Dial("tcp", nodeAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(rootAddress + ":New Fragment:" + strconv.Itoa(id_frag)))
	outConn.Close()
}

// Envoyée par le root pour annoncer la fin
func send_Terminate(rootAddress string, nodeAddress string) {
	if MY_DEBUG {
		printAddress("TERMINATED", rootAddress, nodeAddress)
	}
	outConn, err := net.Dial("tcp", nodeAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(rootAddress + ":Terminated"))
	outConn.Close()
}

// Envoyée en reponse a le message new fragment pour ACK la reception
func send_New_Fragment_ACK(rootAddress string, nodeAddress string) {
	if MY_DEBUG {
		printAddress("NEW FRAG ACK", rootAddress, nodeAddress)
	}
	outConn, err := net.Dial("tcp", nodeAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(rootAddress + ":New Fragment ACK"))
	outConn.Close()
}

// Envoyée par le root pour commencer un nouvelle phase
func send_Start(rootAddress string, nodeAddress string) {
	if MY_DEBUG {
		printAddress("START", rootAddress, nodeAddress)
	}
	outConn, err := net.Dial("tcp", nodeAddress+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}
	outConn.Write([]byte(rootAddress + ":Start"))
	outConn.Close()
}

// func to check if a elemnt is in a list
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func server(neighboursFilePath string, isStartingPoint bool) {
	var node yamlConfig = initAndParseFileNeighbours(neighboursFilePath)
	filename := "Log-" + node.Address
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Log(file, "Parsing done ...\n")
	Log(file, "Server starting ....\n")
	ln, err := net.Listen("tcp", node.Address+PORT)
	if err != nil {
		log.Fatal(err)
		return
	}

	type My_Struct_MWOE struct {
		MWOE         int
		list_address string
	}

	// On fait le sort de la liste de neighbours
	type My_struct struct {
		id                   int
		address              string
		edge_weight          int
		state                string
		possible_double_conn bool
	}

	var My_Neighbours []My_struct
	for _, neigh := range node.Neighbours {
		tmp := My_struct{neigh.ID, neigh.Address, neigh.EdgeWeight, "basic", false}
		My_Neighbours = append(My_Neighbours, tmp)
	}
	sort.SliceStable(My_Neighbours, func(i, j int) bool {
		return My_Neighbours[i].edge_weight < My_Neighbours[j].edge_weight
	})

	oldConnect := struct {
		received bool
		address  string
	}{
		false,
		"",
	}

	var test_sent int = 0          // Variable to check si tous le test envoiee on recu un reponse
	var count_ACK int = 0          // Nombre de ACK recu
	var list_MWOE []My_Struct_MWOE // List de MWOE recu + local
	var count_fils int = 0
	var id_frag int = node.ID
	var parent_addr string = ""
	var local_MWOE int = MAXINT // ça c'est le MWOE for edge_node
	var non_term bool = true    // Variable de terminaison -> true == not terminated, false == terminated
	var add_to_check_double_conn string

	Log(file, "Starting algorithm ...\n")
	// Initialisation de programe chaque est un fragement et elle va envoyer le Test au MWOE
	test_sent += 1
	go send_Test(node.Address, My_Neighbours[0].address, id_frag)

	for non_term {
		conn, _ := ln.Accept()
		message, _ := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		remote_addr := message[0:9]
		msg := message[9:]
		Log(file, "Message received : "+msg+" From "+remote_addr+"\n")
		type_msg := strings.Split(message, ":")[1]
		switch type_msg {
		case "Test":
			// Si le node n'est pas dans le fragment elle va envoyer  accept si non va refuser
			tmp, _ := strconv.Atoi(strings.Split(message, ":")[2])
			if id_frag != tmp {
				send_accept(node.Address, remote_addr, true)
			} else {
				send_accept(node.Address, remote_addr, false)
			}

		case "Accept":
			// Si le node a récu accept elle va chercher parmis le neighbours celui qui a le state basic(c'est pa rejected)
			// apres il  va reporter la valeur de MWOE vers le root

			// Variable de controle pour voire si test envoiee == a accepts recu (ici on diminu la variable, car on a recu un accept)
			test_sent -= 1

			if strings.Split(message, ":")[2] == "true" {
				// Si j'ai un parent je vais l'envoyer un report
				if parent_addr != "" {
					for i, neigh := range My_Neighbours {
						// On peut changer ici la condition sur l'addr remote
						if neigh.address == remote_addr {
							local_MWOE = neigh.edge_weight

							tmp := My_Struct_MWOE{local_MWOE, remote_addr}
							list_MWOE = append(list_MWOE, tmp)

							if count_fils == 0 {
								send_report(node.Address, parent_addr, local_MWOE, neigh.address)
							}
							add_to_check_double_conn = remote_addr
							My_Neighbours[i].possible_double_conn = true
							break
						}
					}
				} else {
					//si non on envoit le connect vers le node qui a envoyé accept
					for i, neigh := range My_Neighbours {
						if neigh.address == remote_addr {
							My_Neighbours[i].state = "branch"
							My_Neighbours[i].possible_double_conn = true
						}
					}
					add_to_check_double_conn = remote_addr
					send_CONNECT(node.Address, remote_addr, node.ID)

				}

			} else {
				// si c'est pas accpeté cad le node appartient au fragment du coup on change son state à rejected
				local_MWOE = MAXINT
				tmp := My_Struct_MWOE{local_MWOE, node.Address}
				list_MWOE = append(list_MWOE, tmp)

				for i, neigh := range My_Neighbours {
					if neigh.address == remote_addr {
						My_Neighbours[i].state = "reject"
					}
					if My_Neighbours[i].state == "basic" {
						test_sent += 1
						send_Test(node.Address, neigh.address, id_frag)
						break
					}
				}

				// Ici on a bien réçu tous les reports de tous les fils
				sort.SliceStable(list_MWOE, func(i, j int) bool {
					return list_MWOE[i].MWOE < list_MWOE[j].MWOE
				})

				var list_term []string
				for _, neigh := range My_Neighbours {
					list_term = append(list_term, neigh.state)
				}
				if !contains(list_term, "basic") && count_fils == 0 {
					send_report(node.Address, parent_addr, list_MWOE[0].MWOE, "")
				}
			}

		case "CONNECT":
			id, _ := strconv.Atoi(strings.Split(message, ":")[2])
			// Si on a la double connexion On change le state de lien entre les 2 noeuds en branch et on choisi le nouveau root
			if parent_addr == "" {
				for i, neigh := range My_Neighbours {
					if neigh.id == id {
						My_Neighbours[i].state = "branch"
					}
				}
			}

			if parent_addr != "" {
				oldConnect.received = true
				oldConnect.address = remote_addr
			}

			// Selection de nouveau root . Celui qui a le poids le plus grand
			if node.ID > id && add_to_check_double_conn == remote_addr {
				Log(file, "I have double connexion with "+strconv.Itoa(id)+"\n")
				rootAddress := node.Address
				id_root := node.ID
				id_frag = id_root

				for _, neigh := range My_Neighbours {
					if neigh.state == "branch" {
						count_fils += 1
						send_New_Fragment(rootAddress, neigh.address, id_root)
					}
				}
			}

		case "New Fragment":
			// On envoit le new fragment vers tous les fils de ce noeud (le fils cad le state de lien = branch
			id_frag, _ = strconv.Atoi(strings.Split(message, ":")[2])
			parent_addr = remote_addr
			for _, neigh := range My_Neighbours {
				if neigh.state == "branch" && neigh.address != parent_addr {
					send_New_Fragment(node.Address, neigh.address, id_frag)
					count_fils += 1
				}
			}
			if count_fils == 0 {
				send_New_Fragment_ACK(node.Address, parent_addr)
			}

		case "Report":
			// Lorsque on recoit un report en décremente le conteur pour s'assurer qu'on a récu les reports de la part de tous les fils
			count_fils -= 1
			// On récupere le MWOE récue et l'ajout à la list de MWOE pour le current node
			MWOE, _ := strconv.Atoi(strings.Split(message, ":")[2])
			list_address := strings.Split(message, ":")[3]

			tmp := My_Struct_MWOE{MWOE, list_address}
			list_MWOE = append(list_MWOE, tmp)

			if count_fils == 0 && test_sent == 0 {
				// Ici on a bien réçu tous les reports de tous les fils et tous le reponse a nos tests
				sort.SliceStable(list_MWOE, func(i, j int) bool {
					return list_MWOE[i].MWOE < list_MWOE[j].MWOE
				})
				if node.ID == id_frag {
					// Si le node est un root alors il va prendre le meilleur MWOE et il envoie le merge vers le edge_node
					// envoie le merge vers le dernier addresse qui me permet de atteindre le edge_node
					list_addre_to_send := strings.Split(list_MWOE[0].list_address, "/")
					node_to_edge := list_addre_to_send[(len(list_addre_to_send) - 1)]

					if list_MWOE[0].MWOE != MAXINT {
						send_Merge(node.Address, node_to_edge, strings.Join(list_addre_to_send[:(len(list_addre_to_send)-1)], "/"))
					} else {
						for _, neigh := range My_Neighbours {
							if neigh.state == "branch" {
								send_Terminate(node.Address, neigh.address)
							}
						}
						non_term = false
					}

				} else {
					// il va acheminer le report vers son parent avec son addresse
					send_report(node.Address, parent_addr, list_MWOE[0].MWOE, list_MWOE[0].list_address)
				}

			}

		case "Merge":
			// Si la longueur de la liste d'addresses = 1 on a bien arrivé au edge node. Dans ce cas on envoi le connect
			list_address := strings.Split(message, ":")[2]

			if oldConnect.received && add_to_check_double_conn == oldConnect.address {
				Log(file, "I have double connexion with "+oldConnect.address+"\n")
				rootAddress := node.Address
				id_root := node.ID
				id_frag = id_root

				for i, neigh := range My_Neighbours {
					if neigh.address == oldConnect.address {
						My_Neighbours[i].state = "branch"
					}
				}

				for _, neigh := range My_Neighbours {
					if neigh.state == "branch" {
						count_fils += 1
						send_New_Fragment(rootAddress, neigh.address, id_root)
					}
				}
			}

			if len(strings.Split(list_address, "/")) == 1 && add_to_check_double_conn == list_address {
				for i, neigh := range My_Neighbours {
					if neigh.address == list_address {
						My_Neighbours[i].state = "branch"
					}
				}
				send_CONNECT(node.Address, list_address, node.ID)
			} else {
				// si non on continue l'envoi de merge selon la liste des addresses qu'on a
				list_addre_to_send := strings.Split(list_address, "/")
				node_to_edge := list_addre_to_send[(len(list_addre_to_send) - 1)]
				send_Merge(node.Address, node_to_edge, strings.Join(list_addre_to_send[:(len(list_addre_to_send)-1)], "/"))
			}

		case "Terminated":
			non_term = false

			for _, neigh := range My_Neighbours {
				if neigh.state == "branch" && neigh.address != parent_addr {
					send_Terminate(node.Address, neigh.address)
				}
			}

		case "New Fragment ACK":
			count_ACK += 1

			if count_ACK >= count_fils {
				if id_frag != node.ID {
					send_New_Fragment_ACK(node.Address, parent_addr)
				} else {

					list_MWOE = nil
					local_MWOE = MAXINT
					tmp := My_Struct_MWOE{local_MWOE, node.Address}
					list_MWOE = append(list_MWOE, tmp)

					for i, neigh := range My_Neighbours {
						if My_Neighbours[i].state == "basic" {
							test_sent += 1
							send_Test(node.Address, neigh.address, id_frag)
							break
						}
					}
					for _, neigh := range My_Neighbours {
						if neigh.state == "branch" {
							send_Start(node.Address, neigh.address)
						}
					}
				}
				count_ACK = 0
			}

		case "Start":

			list_MWOE = nil
			local_MWOE = MAXINT
			tmp := My_Struct_MWOE{local_MWOE, node.Address}
			list_MWOE = append(list_MWOE, tmp)

			var list_term []string
			for _, neigh := range My_Neighbours {
				list_term = append(list_term, neigh.state)
			}
			if !contains(list_term, "basic") && count_fils == 0 {
				send_report(node.Address, parent_addr, local_MWOE, "")
			}

			for i, neigh := range My_Neighbours {
				if My_Neighbours[i].state == "basic" {
					test_sent += 1
					send_Test(node.Address, neigh.address, id_frag)
					break
				}
			}

			for _, neigh := range My_Neighbours {
				if neigh.state == "branch" && neigh.address != parent_addr {
					send_Start(node.Address, neigh.address)
				}
			}
		}
	}

	for _, neigh := range My_Neighbours {
		Log(file, "Node ID: "+strconv.Itoa(node.ID)+
			"\n     Neigbours Node ID: "+strconv.Itoa(neigh.id)+
			"\n     Neigbours Edge weight: "+strconv.Itoa(neigh.edge_weight)+
			"\n     Neigbours Edge state: "+neigh.state+
			"\n     Fragment ID: "+strconv.Itoa(id_frag)+"\n")
	}

	if PRINT_NEIGH_LIST {
		for _, neigh := range My_Neighbours {
			fmt.Println("Node ID: ", strconv.Itoa(node.ID),
				"\n     Neigbours Node ID: ", strconv.Itoa(neigh.id),
				"\n     Neigbours Edge weight: ", strconv.Itoa(neigh.edge_weight),
				"\n     Neigbours Edge state: ", neigh.state,
				"\n     Fragment ID: ", strconv.Itoa(id_frag))
		}
	}
}

func main() {
	//localadress := "127.0.0.1"
	go server("Neighbours/node-2.yaml", false)
	go server("Neighbours/node-3.yaml", false)
	go server("Neighbours/node-4.yaml", false)
	go server("Neighbours/node-5.yaml", false)
	go server("Neighbours/node-6.yaml", false)
	go server("Neighbours/node-7.yaml", false)
	go server("Neighbours/node-8.yaml", false)
	go server("Neighbours/node-1.yaml", false)

	time.Sleep(5 * time.Second) //Waiting all console return from nodes
}
