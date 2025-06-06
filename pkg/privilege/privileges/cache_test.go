// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package privileges_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestLoadUserTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql;")
	tk.MustExec("truncate table user;")

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	require.Len(t, p.User(), 0)

	// Host | User | authentication_string | Select_priv | Insert_priv | Update_priv | Delete_priv | Create_priv | Drop_priv | Process_priv | Grant_priv | References_priv | Alter_priv | Show_db_priv | Super_priv | Execute_priv | Index_priv | Create_user_priv | Trigger_priv
	tk.MustExec(`INSERT INTO mysql.user (Host, User, authentication_string, Select_priv) VALUES ("%", "root", "", "Y")`)
	tk.MustExec(`INSERT INTO mysql.user (Host, User, authentication_string, Insert_priv) VALUES ("%", "root1", "admin", "Y")`)
	tk.MustExec(`INSERT INTO mysql.user (Host, User, authentication_string, Update_priv, Show_db_priv, References_priv) VALUES ("%", "root11", "", "Y", "Y", "Y")`)
	tk.MustExec(`INSERT INTO mysql.user (Host, User, authentication_string, Create_user_priv, Index_priv, Execute_priv, Create_view_priv, Show_view_priv, Show_db_priv, Super_priv, Trigger_priv) VALUES ("%", "root111", "", "Y",  "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)
	tk.MustExec(`INSERT INTO mysql.user (Host, User, user_attributes, token_issuer) VALUES ("%", "root1111", "{\"metadata\": {\"email\": \"user@pingcap.com\"}}", "<token-issuer>")`)
	tk.MustExec(`INSERT INTO mysql.user (Host, User, password_expired, password_last_changed, password_lifetime) VALUES ("%", "root2", "Y", "2022-10-10 12:00:00", 3)`)
	tk.MustExec(`INSERT INTO mysql.user (Host, User, password_expired, password_last_changed) VALUES ("%", "root3", "N", "2022-10-10 12:00:00")`)

	p = privileges.NewMySQLPrivilege()
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	// require.Len(t, p.User(), len(p.UserMap))

	user := p.User()
	require.Equal(t, "root", user[0].User)
	require.Equal(t, mysql.SelectPriv, user[0].Privileges)
	require.Equal(t, mysql.InsertPriv, user[1].Privileges)
	require.Equal(t, mysql.UpdatePriv|mysql.ShowDBPriv|mysql.ReferencesPriv, user[2].Privileges)
	require.Equal(t, mysql.CreateUserPriv|mysql.IndexPriv|mysql.ExecutePriv|mysql.CreateViewPriv|mysql.ShowViewPriv|mysql.ShowDBPriv|mysql.SuperPriv|mysql.TriggerPriv, user[3].Privileges)
	require.Equal(t, "user@pingcap.com", user[4].Email)
	require.Equal(t, "<token-issuer>", user[4].AuthTokenIssuer)
	require.Equal(t, true, user[5].PasswordExpired)
	require.Equal(t, time.Date(2022, 10, 10, 12, 0, 0, 0, time.Local), user[5].PasswordLastChanged)
	require.Equal(t, int64(3), user[5].PasswordLifeTime)
	require.Equal(t, false, user[6].PasswordExpired)
	require.Equal(t, time.Date(2022, 10, 10, 12, 0, 0, 0, time.Local), user[6].PasswordLastChanged)
	require.Equal(t, int64(-1), user[6].PasswordLifeTime)

	// test switching default auth plugin
	for _, plugin := range []string{mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password} {
		p = privileges.NewMySQLPrivilege()
		p.SetGlobalVarsAccessor(se.GetSessionVars().GlobalVarsAccessor)
		require.NoError(t, se.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.DefaultAuthPlugin, plugin))
		require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
		require.Equal(t, plugin, p.User()[0].AuthPlugin)
	}
}

func TestLoadGlobalPrivTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql;")
	tk.MustExec("truncate table global_priv")

	tk.MustExec(`INSERT INTO mysql.global_priv VALUES ("%", "tu", "{\"access\":0,\"plugin\":\"mysql_native_password\",\"ssl_type\":3,
				\"ssl_cipher\":\"cipher\",\"x509_subject\":\"\C=ZH1\", \"x509_issuer\":\"\C=ZH2\", \"san\":\"\IP:127.0.0.1, IP:1.1.1.1, DNS:pingcap.com, URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1\", \"password_last_changed\":1}")`)

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadGlobalPrivTable(se.GetSQLExecutor()))
	val := p.GlobalPriv("tu")[0]
	require.Equal(t, `%`, val.Host)
	require.Equal(t, `tu`, val.User)
	require.Equal(t, privileges.SslTypeSpecified, val.Priv.SSLType)
	require.Equal(t, "C=ZH2", val.Priv.X509Issuer)
	require.Equal(t, "C=ZH1", val.Priv.X509Subject)
	require.Equal(t, "IP:127.0.0.1, IP:1.1.1.1, DNS:pingcap.com, URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1", val.Priv.SAN)
	require.Len(t, val.Priv.SANs[util.IP], 2)
	require.Equal(t, "pingcap.com", val.Priv.SANs[util.DNS][0])
	require.Equal(t, "spiffe://mesh.pingcap.com/ns/timesh/sa/me1", val.Priv.SANs[util.URI][0])
}

func TestLoadDBTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql;")
	tk.MustExec("truncate table db;")

	tk.MustExec(`INSERT INTO mysql.db (Host, DB, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv) VALUES ("%", "information_schema", "root", "Y", "Y", "Y", "Y", "Y")`)
	tk.MustExec(`INSERT INTO mysql.db (Host, DB, User, Drop_priv, Grant_priv, Index_priv, Alter_priv, Create_view_priv, Show_view_priv, Execute_priv) VALUES ("%", "mysql", "root1", "Y", "Y", "Y", "Y", "Y", "Y", "Y")`)

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadDBTable(se.GetSQLExecutor()))
	// require.Len(t, p.DB(), len(p.DBMap))

	require.Equal(t, mysql.SelectPriv|mysql.InsertPriv|mysql.UpdatePriv|mysql.DeletePriv|mysql.CreatePriv, p.DB()[0].Privileges)
	require.Equal(t, mysql.DropPriv|mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv|mysql.CreateViewPriv|mysql.ShowViewPriv|mysql.ExecutePriv, p.DB()[1].Privileges)
}

func TestLoadTablesPrivTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql;")
	tk.MustExec("truncate table tables_priv")

	tk.MustExec(`INSERT INTO mysql.tables_priv VALUES ("%", "db", "user", "table", "grantor", "2017-01-04 16:33:42.235831", "Grant,Index,Alter", "Insert,Update")`)

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadTablesPrivTable(se.GetSQLExecutor()))
	tablesPriv := p.TablesPriv()
	// require.Len(t, tablesPriv, len(p.TablesPrivMap))

	require.Equal(t, `%`, tablesPriv[0].Host)
	require.Equal(t, "db", tablesPriv[0].DB)
	require.Equal(t, "user", tablesPriv[0].User)
	require.Equal(t, "table", tablesPriv[0].TableName)
	require.Equal(t, mysql.GrantPriv|mysql.IndexPriv|mysql.AlterPriv, tablesPriv[0].TablePriv)
	require.Equal(t, mysql.InsertPriv|mysql.UpdatePriv, tablesPriv[0].ColumnPriv)
}

func TestLoadColumnsPrivTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql;")
	tk.MustExec("truncate table columns_priv")

	tk.MustExec(`INSERT INTO mysql.columns_priv VALUES ("%", "db", "user", "table", "column", "2017-01-04 16:33:42.235831", "Insert,Update")`)
	tk.MustExec(`INSERT INTO mysql.columns_priv VALUES ("127.0.0.1", "db", "user", "table", "column", "2017-01-04 16:33:42.235831", "Select")`)

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadColumnsPrivTable(se.GetSQLExecutor()))
	columnsPriv := p.ColumnsPriv()
	require.Equal(t, `%`, columnsPriv[0].Host)
	require.Equal(t, "db", columnsPriv[0].DB)
	require.Equal(t, "user", columnsPriv[0].User)
	require.Equal(t, "table", columnsPriv[0].TableName)
	require.Equal(t, "column", columnsPriv[0].ColumnName)
	require.Equal(t, mysql.InsertPriv|mysql.UpdatePriv, columnsPriv[0].ColumnPriv)
	require.Equal(t, mysql.SelectPriv, columnsPriv[1].ColumnPriv)
}

func TestLoadDefaultRoleTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql;")
	tk.MustExec("truncate table default_roles")

	tk.MustExec(`INSERT INTO mysql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_1")`)
	tk.MustExec(`INSERT INTO mysql.default_roles VALUES ("%", "test_default_roles", "localhost", "r_2")`)
	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadDefaultRoles(se.GetSQLExecutor()))
	require.Equal(t, `%`, p.DefaultRoles()[0].Host)
	require.Equal(t, "test_default_roles", p.DefaultRoles()[0].User)
	require.Equal(t, "localhost", p.DefaultRoles()[0].DefaultRoleHost)
	require.Equal(t, "r_1", p.DefaultRoles()[0].DefaultRoleUser)
	require.Equal(t, "localhost", p.DefaultRoles()[1].DefaultRoleHost)
}

func TestPatternMatch(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	activeRoles := make([]*auth.RoleIdentity, 0)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE MYSQL;")
	tk.MustExec("TRUNCATE TABLE mysql.user")
	tk.MustExec(`INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("10.0.%", "root", "Y", "Y")`)
	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	require.True(t, p.RequestVerification(activeRoles, "root", "10.0.1", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "114.114.114.114", "test", "", "", mysql.PrivilegeType(0)))
	require.True(t, p.RequestVerification(activeRoles, "root", "10.0.1.118", "test", "", "", mysql.ShutdownPriv))

	tk.MustExec("TRUNCATE TABLE mysql.user")
	tk.MustExec(`INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("", "root", "Y", "N")`)
	p = privileges.NewMySQLPrivilege()
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	require.True(t, p.RequestVerification(activeRoles, "root", "", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "notnull", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "", "test", "", "", mysql.ShutdownPriv))

	// Pattern match for DB.
	tk.MustExec("TRUNCATE TABLE mysql.user")
	tk.MustExec("TRUNCATE TABLE mysql.db")
	tk.MustExec(`INSERT INTO mysql.db (user,host,db,select_priv) values ('genius', '%', 'te%', 'Y')`)
	require.NoError(t, p.LoadDBTable(se.GetSQLExecutor()))
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "test", "", "", mysql.SelectPriv))
}

func TestHostMatch(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	activeRoles := make([]*auth.RoleIdentity, 0)

	tk := testkit.NewTestKit(t, store)
	// Host name can be IPv4 address + netmask.
	tk.MustExec("USE MYSQL;")
	tk.MustExec("TRUNCATE TABLE mysql.user")
	tk.MustExec(`INSERT INTO mysql.user (HOST, USER, authentication_string, Select_priv, Shutdown_priv) VALUES ("172.0.0.0/255.0.0.0", "root", "", "Y", "Y")`)
	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	require.True(t, p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "172.1.1.1", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "root", "198.0.0.1", "test", "", "", mysql.PrivilegeType(0)))
	require.True(t, p.RequestVerification(activeRoles, "root", "172.0.0.1", "test", "", "", mysql.ShutdownPriv))
	tk.MustExec(`TRUNCATE TABLE mysql.user`)

	// Invalid host name, the user can be created, but cannot login.
	cases := []string{
		"127.0.0.0/24",
		"127.0.0.1/255.0.0.0",
		"127.0.0.0/255.0.0",
		"127.0.0.0/255.0.0.0.0",
		"127%/255.0.0.0",
		"127.0.0.0/%",
		"127.0.0.%/%",
		"127%/%",
	}
	for _, IPMask := range cases {
		sql := fmt.Sprintf(`INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("%s", "root", "Y", "Y")`, IPMask)
		tk.MustExec(sql)
		p := privileges.NewMySQLPrivilege()
		se := tk.Session()
		require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
		require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.1", "test", "", "", mysql.SelectPriv), fmt.Sprintf("test case: %s", IPMask))
		require.False(t, p.RequestVerification(activeRoles, "root", "127.0.0.0", "test", "", "", mysql.SelectPriv), fmt.Sprintf("test case: %s", IPMask))
		require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.ShutdownPriv), fmt.Sprintf("test case: %s", IPMask))
	}

	// Netmask notation cannot be used for IPv6 addresses.
	tk.MustExec(`INSERT INTO mysql.user (HOST, USER, Select_priv, Shutdown_priv) VALUES ("2001:db8::/ffff:ffff::", "root", "Y", "Y")`)
	p = privileges.NewMySQLPrivilege()
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	require.False(t, p.RequestVerification(activeRoles, "root", "2001:db8::1234", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "2001:db8::", "test", "", "", mysql.SelectPriv))
	require.False(t, p.RequestVerification(activeRoles, "root", "localhost", "test", "", "", mysql.ShutdownPriv))
}

func TestCaseInsensitive(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	activeRoles := make([]*auth.RoleIdentity, 0)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE TCTrain;")
	tk.MustExec("CREATE TABLE TCTrain.TCTrainOrder (id int);")
	tk.MustExec("TRUNCATE TABLE mysql.user")
	tk.MustExec(`INSERT INTO mysql.db VALUES ("127.0.0.1", "TCTrain", "genius", "Y", "Y", "Y", "Y", "Y", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N")`)
	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadDBTable(se.GetSQLExecutor()))
	// DB and Table names are case-insensitive in MySQL.
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTrain", "TCTrainOrder", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "TCTRAIN", "TCTRAINORDER", "", mysql.SelectPriv))
	require.True(t, p.RequestVerification(activeRoles, "genius", "127.0.0.1", "tctrain", "tctrainorder", "", mysql.SelectPriv))
}

func TestLoadRoleGraph(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use mysql;")
	tk.MustExec("truncate table user;")

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadDBTable(se.GetSQLExecutor()))
	require.Len(t, p.User(), 0)

	tk.MustExec(`INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_1", "%", "user2")`)
	tk.MustExec(`INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_2", "%", "root")`)
	tk.MustExec(`INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_3", "%", "user1")`)
	tk.MustExec(`INSERT INTO mysql.role_edges (FROM_HOST, FROM_USER, TO_HOST, TO_USER) VALUES ("%", "r_4", "%", "root")`)

	p = privileges.NewMySQLPrivilege()
	require.NoError(t, p.LoadRoleGraph(se.GetSQLExecutor()))
	graph := p.RoleGraph()
	require.True(t, graph[auth.RoleIdentity{Username: "root", Hostname: "%"}].Find("r_2", "%"))
	require.True(t, graph[auth.RoleIdentity{Username: "root", Hostname: "%"}].Find("r_4", "%"))
	require.True(t, graph[auth.RoleIdentity{Username: "user2", Hostname: "%"}].Find("r_1", "%"))
	require.True(t, graph[auth.RoleIdentity{Username: "user1", Hostname: "%"}].Find("r_3", "%"))
	_, ok := graph[auth.RoleIdentity{Username: "illedal"}]
	require.False(t, ok)
	require.False(t, graph[auth.RoleIdentity{Username: "root", Hostname: "%"}].Find("r_1", "%"))
}

func TestRoleGraphBFS(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE ROLE r_1, r_2, r_3, r_4, r_5, r_6;`)
	tk.MustExec(`GRANT r_2 TO r_1;`)
	tk.MustExec(`GRANT r_3 TO r_2;`)
	tk.MustExec(`GRANT r_4 TO r_3;`)
	tk.MustExec(`GRANT r_1 TO r_4;`)
	tk.MustExec(`GRANT r_5 TO r_3, r_6;`)

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadRoleGraph(se.GetSQLExecutor()))

	activeRoles := make([]*auth.RoleIdentity, 0)
	ret := p.FindAllRole(activeRoles)
	require.Len(t, ret, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_1", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	require.Len(t, ret, 5)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	require.Len(t, ret, 2)

	activeRoles = make([]*auth.RoleIdentity, 0)
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_3", Hostname: "%"})
	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "r_6", Hostname: "%"})
	ret = p.FindAllRole(activeRoles)
	require.Len(t, ret, 6)
}

func TestFindAllUserEffectiveRoles(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER u1`)
	tk.MustExec(`CREATE ROLE r_1, r_2, r_3, r_4;`)
	tk.MustExec(`GRANT r_3 to r_1`)
	tk.MustExec(`GRANT r_4 to r_2`)
	tk.MustExec(`GRANT r_1 to u1`)
	tk.MustExec(`GRANT r_2 to u1`)

	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadAll(se.GetSQLExecutor()))
	ret := p.FindAllUserEffectiveRoles("u1", "%", []*auth.RoleIdentity{
		{Username: "r_1", Hostname: "%"},
		{Username: "r_2", Hostname: "%"},
	})
	require.Equal(t, 4, len(ret))
	require.Equal(t, "r_1", ret[0].Username)
	require.Equal(t, "r_2", ret[1].Username)
	require.Equal(t, "r_3", ret[2].Username)
	require.Equal(t, "r_4", ret[3].Username)

	tk.MustExec(`REVOKE r_2 from u1`)
	require.NoError(t, p.LoadAll(se.GetSQLExecutor()))
	ret = p.FindAllUserEffectiveRoles("u1", "%", []*auth.RoleIdentity{
		{Username: "r_1", Hostname: "%"},
		{Username: "r_2", Hostname: "%"},
	})
	require.Equal(t, 2, len(ret))
	require.Equal(t, "r_1", ret[0].Username)
	require.Equal(t, "r_3", ret[1].Username)
}

func TestSortUserTable(t *testing.T) {
	p := privileges.NewMySQLPrivilege()
	p.SetUser([]privileges.UserRecord{
		privileges.NewUserRecord(`%`, "root"),
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord("localhost", "root"),
		privileges.NewUserRecord("localhost", ""),
	})
	p.SortUserTable()
	result := []privileges.UserRecord{
		privileges.NewUserRecord("localhost", ""),
		privileges.NewUserRecord("localhost", "root"),
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord(`%`, "root"),
	}
	checkUserRecord(t, p.User(), result)

	p.SetUser([]privileges.UserRecord{
		privileges.NewUserRecord(`%`, "jeffrey"),
		privileges.NewUserRecord("h1.example.net", ""),
	})
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord("h1.example.net", ""),
		privileges.NewUserRecord(`%`, "jeffrey"),
	}
	checkUserRecord(t, p.User(), result)

	p.SetUser([]privileges.UserRecord{
		privileges.NewUserRecord(`192.168.%`, "xxx"),
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
	})
	p.SortUserTable()
	result = []privileges.UserRecord{
		privileges.NewUserRecord(`192.168.199.%`, "xxx"),
		privileges.NewUserRecord(`192.168.%`, "xxx"),
	}
	checkUserRecord(t, p.User(), result)
}

func TestGlobalPrivValueRequireStr(t *testing.T) {
	var (
		none  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeNone}
		tls   = privileges.GlobalPrivValue{SSLType: privileges.SslTypeAny}
		x509  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeX509}
		spec  = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, SSLCipher: "c1", X509Subject: "s1", X509Issuer: "i1"}
		spec2 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Subject: "s1", X509Issuer: "i1"}
		spec3 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified, X509Issuer: "i1"}
		spec4 = privileges.GlobalPrivValue{SSLType: privileges.SslTypeSpecified}
	)
	require.Equal(t, "NONE", none.RequireStr())
	require.Equal(t, "SSL", tls.RequireStr())
	require.Equal(t, "X509", x509.RequireStr())
	require.Equal(t, "CIPHER 'c1' ISSUER 'i1' SUBJECT 's1'", spec.RequireStr())
	require.Equal(t, "ISSUER 'i1' SUBJECT 's1'", spec2.RequireStr())
	require.Equal(t, "ISSUER 'i1'", spec3.RequireStr())
	require.Equal(t, "NONE", spec4.RequireStr())
}

func checkUserRecord(t *testing.T, x, y []privileges.UserRecord) {
	require.Equal(t, len(x), len(y))
	for i := range x {
		require.Equal(t, x[i].User, y[i].User)
		require.Equal(t, x[i].Host, y[i].Host)
	}
}

func TestDBIsVisible(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database visdb")
	p := privileges.NewMySQLPrivilege()
	se := tk.Session()
	require.NoError(t, p.LoadAll(se.GetSQLExecutor()))

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Create_role_priv, Super_priv) VALUES ("%", "testvisdb", "Y", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible := p.DBIsVisible("testvisdb", "%", "visdb")
	require.False(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Select_priv) VALUES ("%", "testvisdb2", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb2", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Create_priv) VALUES ("%", "testvisdb3", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb3", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Insert_priv) VALUES ("%", "testvisdb4", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb4", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Update_priv) VALUES ("%", "testvisdb5", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb5", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Create_view_priv) VALUES ("%", "testvisdb6", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb6", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Trigger_priv) VALUES ("%", "testvisdb7", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb7", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, References_priv) VALUES ("%", "testvisdb8", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb8", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")

	tk.MustExec(`INSERT INTO mysql.user (Host, User, Execute_priv) VALUES ("%", "testvisdb9", "Y")`)
	require.NoError(t, p.LoadUserTable(se.GetSQLExecutor()))
	isVisible = p.DBIsVisible("testvisdb9", "%", "visdb")
	require.True(t, isVisible)
	tk.MustExec("TRUNCATE TABLE mysql.user")
}
