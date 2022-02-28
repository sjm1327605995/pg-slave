package client

import (
	"context"
	"github.com/jackc/pgx"
	"log"
	"pg-Slave/conf"
)

type Slave struct {
	URL             string
	Slot            string
	Plugin          string
	ReplicationConn *pgx.ReplicationConn
	LSN             uint64
	connConfig      pgx.ConnConfig
	conn            *pgx.Conn
}

func Connect() (s *Slave, err error) {
	s = new(Slave)
	s.URL = conf.Conf.MasterURL
	s.Slot = conf.Conf.Slot
	s.Plugin = conf.Conf.Plugin
	connConfig, err := pgx.ParseURI(s.URL)
	if err != nil {
		return nil, err
	}
	if connConfig.User == "" {
		connConfig.User = conf.Conf.User
	}
	if connConfig.Password == "" {
		connConfig.Password = conf.Conf.Password
	}
	s.connConfig = connConfig
	conn, err := pgx.Connect(connConfig)
	if err != nil {
		return nil, err
	}
	s.conn = conn
	s.ReplicationConn, err = pgx.ReplicationConnect(connConfig)
	if err != nil {
		return nil, err
	}

	return s, err
}
func (s *Slave) StartReplication() {
	if err := s.ReplicationConn.StartReplication(s.Slot, 0, -1); err != nil {
		log.Fatalf("fail to start replication on slot %s : %s", s.Slot, err.Error())
	}
}
func (s *Slave) DropReplicationSlot() (err error) {
	conn := s.conn
	var slotExists bool

	err = conn.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, s.Slot).Scan(&slotExists)
	if err != nil {
		return err
	}
	if slotExists {
		if s.ReplicationConn != nil {
			_ = s.ReplicationConn.Close()
		}
		_, err = conn.Exec("SELECT pg_drop_replication_slot($1)", s.Slot)
		if err != nil {
			return err
		}
		log.Printf("drop replication slot %s", s.Slot)
	}
	return nil
}

func (s *Slave) CreateReplicationSlot() (err error) {
	if consistPoint, snapshotName, err := s.ReplicationConn.CreateReplicationSlotEx(s.Slot, s.Plugin); err != nil {
		log.Fatalf("fail to create replication slot: %s", err.Error())
		return err
	} else {
		log.Printf("create replication slot %s with plugin %s : consist snapshot: %s, snapshot name: %s",
			s.Slot, s.Plugin, consistPoint, snapshotName)
		if s.LSN, err = pgx.ParseLSN(consistPoint); err != nil {
			return err
		}
	}
	return nil
}
func (s *Slave) Subscribe() {

	var message *pgx.ReplicationMessage

	for {

		message, _ = s.ReplicationConn.WaitForReplicationMessage(context.Background())
		if message.WalMessage != nil {
			DoSomething(message.WalMessage)          // 如果是真的消息就消费它
			if message.WalMessage.WalStart > s.LSN { // 消费完后更新消费进度，并向主库汇报
				s.LSN = message.WalMessage.WalStart + uint64(len(message.WalMessage.WalData))
				if err := s.ReportProgress(); err != nil { // 如果服务器心跳包要求回送进度，则汇报进度
					log.Fatalf("fail to create replication slot: %s", err.Error())
				}
			}
		}
		if message.ServerHeartbeat != nil && message.ServerHeartbeat.ReplyRequested == 1 {
			if err := s.ReportProgress(); err != nil { // 如果服务器心跳包要求回送进度，则汇报进度
				log.Fatalf("fail to create replication slot: %s", err.Error())
			}
		}
	}
}
func DoSomething(message *pgx.WalMessage) {
	log.Printf("[LSN] %s [Payload] %s",
		pgx.FormatLSN(message.WalStart), string(message.WalData))
}
func (s *Slave) ReportProgress() (err error) {
	status, err := pgx.NewStandbyStatus(s.LSN)
	if err != nil {
		return
	}
	err = s.ReplicationConn.SendStandbyStatus(status)
	if err != nil {
		return
	}
	return nil
}

type Payload struct {
	Change []struct {
		Kind         string        `json:"kind"`
		Schema       string        `json:"schema"`
		Table        string        `json:"table"`
		ColumnNames  []string      `json:"columnnames"`
		ColumnTypes  []string      `json:"columntypes"`
		ColumnValues []interface{} `json:"columnvalues"`
		OldKeys      struct {
			KeyNames  []string      `json:"keynames"`
			KeyTypes  []string      `json:"keytypes"`
			KeyValues []interface{} `json:"keyvalues"`
		} `json:"oldkeys"`
	} `json:"change"`
}
