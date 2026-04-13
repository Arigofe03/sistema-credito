import streamlit as st
import psycopg2
import pandas as pd
import datetime
import tempfile
import json
import time
from fpdf import FPDF
import plotly.express as px

# --- CONFIGURAÇÃO DA PÁGINA E OCULTAÇÃO DO "CARREGANDO" ---
st.set_page_config(page_title="Sistema de Gestão de Vendas", layout="wide")
st.markdown("""
    <style>
        [data-testid="stStatusWidget"] {display: none !important;}
    </style>
""", unsafe_allow_html=True)

# --- CONEXÃO COM O BANCO DE DADOS NEON ---
DATABASE_URL = st.secrets["DB_URL"]

def conectar_banco():
    return psycopg2.connect(DATABASE_URL)

# --- FUNÇÃO PARA FORMATAR MOEDA NO PADRÃO BRASILEIRO ---
def formatar_moeda(valor):
    if pd.isna(valor) or valor is None:
        return "R$ 0,00"
    return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# --- FUNÇÃO DE CÁLCULO DE BÔNUS FIDELIDADE ---
def calcular_bonus(valor):
    if valor < 500.00:
        return 0.0
    elif valor >= 500.00 and valor < 1000.00:
        return 15.0
    elif valor >= 1000.00 and valor < 2000.00:
        return 20.0
    elif valor >= 2000.00 and valor < 3000.00:
        return 40.0
    elif valor >= 3000.00 and valor < 4000.00:
        return 60.0
    elif valor >= 4000.00 and valor < 5000.00:
        return 80.0
    else:
        milhares = int(valor // 1000)
        return float(milhares * 20.0)

# --- LISTAS PADRÕES DO SISTEMA ---
LISTA_LOJAS = ["Berimbau", "Centro", "Sussuarana", "Irará", "Liberdade", "Iapi"]
LISTA_PARCELAS = ["Débito", "1x", "2x", "3x", "4x", "5x", "6x", "7x", "8x", "9x", "10x", "11x", "12x", "13x", "14x", "15x", "16x", "17x", "18x"]
LISTA_BANDEIRAS_ATENDENTE = ["Selecione...", "Visa/Mastercard", "Elo/Hiper/Demais", "Visa", "Mastercard", "Elo", "Hipercard", "American Express", "Outra"]
LISTA_BANDEIRAS_ADMIN = ["Visa/Mastercard", "Elo/Hiper/Demais", "Visa", "Mastercard", "Elo", "Hipercard", "American Express", "Outra"]

DADOS_TAXAS_PADRAO = [
    ("Débito", 0.99, 1.60), ("1x", 2.99, 3.99), ("2x", 4.09, 5.30),
    ("3x", 4.78, 5.99), ("4x", 5.47, 6.68), ("5x", 6.14, 7.35),
    ("6x", 6.81, 8.02), ("7x", 7.67, 9.47), ("8x", 8.33, 10.13),
    ("9x", 8.98, 10.78), ("10x", 9.63, 11.43), ("11x", 10.26, 12.06),
    ("12x", 10.90, 12.70), ("13x", 12.32, 13.32), ("14x", 12.94, 13.94),
    ("15x", 13.56, 14.56), ("16x", 14.17, 15.17), ("17x", 14.77, 15.77),
    ("18x", 15.37, 16.37)
]

# =====================================================================
# SISTEMA DE CACHE DE ALTA VELOCIDADE
# =====================================================================
@st.cache_resource(show_spinner=False)
def inicializar_banco_uma_vez():
    try:
        conn = conectar_banco()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS contas_pix (id SERIAL PRIMARY KEY, nome_conta VARCHAR(50) UNIQUE NOT NULL);")
        cursor.execute("ALTER TABLE contas_pix ADD COLUMN IF NOT EXISTS saldo_inicial NUMERIC(15,2) DEFAULT 0.0;")
        cursor.execute("CREATE TABLE IF NOT EXISTS entradas_pix (id SERIAL PRIMARY KEY, conta_nome VARCHAR(50) NOT NULL, data_entrada DATE, valor NUMERIC(15,2), descricao TEXT);")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS taxas_cartoes_v2 (
                id SERIAL PRIMARY KEY,
                nome_maquina VARCHAR(50) NOT NULL,
                bandeira VARCHAR(50) NOT NULL,
                parcelas VARCHAR(20) NOT NULL,
                taxa_percentual NUMERIC(5,2) NOT NULL,
                UNIQUE(nome_maquina, bandeira, parcelas)
            );
        """)
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'Pendente';")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS motivo_recusa TEXT;")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS bandeira_cartao VARCHAR(50) DEFAULT 'Não Informada';")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS chave_pix_cliente VARCHAR(100) DEFAULT 'Não Informada';")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS detalhes_cartoes TEXT;")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS detalhes_pagamentos TEXT;")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS bonus_fidelidade NUMERIC(15,2) DEFAULT 0.0;")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS usou_fidelidade BOOLEAN DEFAULT FALSE;")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS fechado_por VARCHAR(100);")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS data_fechamento TIMESTAMP;")
        # ✅ MELHORIA 4: nova coluna para armazenar o valor recebido via PagSeguro
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS valor_pagseguro NUMERIC(15,2) DEFAULT NULL;")
        cursor.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS salario NUMERIC(15,2);")
        cursor.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS data_inicio DATE;")
        cursor.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS data_fim DATE;")
        cursor.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS endereco TEXT;")
        cursor.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS rg VARCHAR(20);")
        cursor.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS cpf VARCHAR(20);")
        cursor.execute("UPDATE vendas SET status = 'Fechada' WHERE total_lucro IS NOT NULL AND status IS NULL;")
        cursor.execute("UPDATE vendas SET status = 'Pendente' WHERE total_lucro IS NULL AND status IS NULL;")
        conn.commit()
        conn.close()
    except Exception as e:
        pass

inicializar_banco_uma_vez()

@st.cache_data(ttl=300, show_spinner=False)
def obter_lista_maquinas_rapido():
    try:
        conn = conectar_banco()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT nome_maquina FROM taxas_cartoes_v2 WHERE nome_maquina != 'Múltiplas'")
        resultados = cursor.fetchall()
        conn.close()
        maquinas_db = [r[0] for r in resultados]
        padroes = ["Silvio", "Naiara", "Moderninha", "Mercado Pago", "Ton", "Outra"]
        todas = list(set(padroes + maquinas_db))
        todas.sort()
        return todas
    except:
        return ["Silvio", "Naiara", "Moderninha", "Mercado Pago", "Ton", "Outra"]

@st.cache_data(ttl=300, show_spinner=False)
def carregar_tabela_taxas_rapido():
    try:
        conn = conectar_banco()
        df = pd.read_sql_query("SELECT nome_maquina, bandeira, parcelas, taxa_percentual FROM taxas_cartoes_v2", conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()

# =====================================================================

# --- FUNÇÃO AUXILIAR PARA GERAR PDF ---
def gerar_pdf(df):
    pdf = FPDF('L', 'mm', 'A4')
    pdf.add_page()
    pdf.set_font('Arial', 'B', 14)
    titulo = 'Relatório Completo de Vendas'.encode('latin-1', 'replace').decode('latin-1')
    pdf.cell(0, 10, titulo, ln=True, align='C')
    pdf.ln(5)
    pdf.set_font('Arial', 'B', 6)
    colunas = list(df.columns)
    larguras = [8, 15, 15, 20, 25, 20, 25, 15, 15, 18, 18, 12, 15, 15, 18, 15, 20, 25] 
    for i, col in enumerate(colunas):
        if i < len(larguras):
            texto_col = str(col).encode('latin-1', 'replace').decode('latin-1')
            pdf.cell(larguras[i], 8, texto_col, border=1, align='C')
    pdf.ln()
    pdf.set_font('Arial', '', 6)
    for index, row in df.iterrows():
        for i, val in enumerate(row):
            if i < len(larguras):
                texto_val = str(val)[:30].encode('latin-1', 'replace').decode('latin-1')
                pdf.cell(larguras[i], 8, texto_val, border=1, align='C')
        pdf.ln()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        pdf.output(tmp.name)
        with open(tmp.name, "rb") as f:
            return f.read()

# --- FUNÇÃO DE CONSULTA DE PERFIL ---
def consultar_perfil_cliente(cpf_busca):
    try:
        conn = conectar_banco()
        query = "SELECT to_char(v.data_venda, 'DD/MM/YYYY') as \"Data\", u.loja as \"Loja\", v.valor_venda as \"Valor\", v.parcelas as \"Parcelas\", v.status as \"Status\", v.usou_fidelidade FROM vendas v JOIN usuarios u ON v.usuario_id = u.id WHERE v.cliente_cpf = %s ORDER BY v.id DESC"
        df_cliente = pd.read_sql_query(query, conn, params=(cpf_busca,))
        cursor = conn.cursor()
        cursor.execute("SELECT cliente_nome FROM vendas WHERE cliente_cpf = %s ORDER BY id DESC LIMIT 1", (cpf_busca,))
        nome_resultado = cursor.fetchone()
        nome_cliente = nome_resultado[0] if nome_resultado else "Desconhecido"
        conn.close()
        
        usou_fid = False
        if not df_cliente.empty and 'usou_fidelidade' in df_cliente.columns:
            usou_fid = df_cliente['usou_fidelidade'].any()
            df_cliente_display = df_cliente.drop(columns=['usou_fidelidade'])
        else:
            df_cliente_display = df_cliente
            
        if df_cliente.empty:
            return None, "Não Encontrado", df_cliente_display
            
        total_operacoes = len(df_cliente)
        valor_total = df_cliente['Valor'].sum()
        recusadas = len(df_cliente[df_cliente['Status'] == 'Recusada'])
        aprovadas = len(df_cliente[df_cliente['Status'] == 'Fechada'])
        
        if recusadas > 0 and aprovadas == 0: perfil = "⚠️ Risco Alto"
        elif total_operacoes >= 5 or valor_total >= 10000: perfil = "🌟 VIP / Alto Valor"
        elif aprovadas > 1: perfil = "🔄 Cliente Frequente"
        else: perfil = "🆕 Cliente Novo"
            
        if usou_fid:
            perfil += " | 👑 Fidelidade"
            
        resumo = {"Nome": nome_cliente, "Total de Tentativas": total_operacoes, "Operações Aprovadas": aprovadas, "Operações Recusadas": recusadas, "Volume Movimentado": valor_total}
        df_cliente_display['Valor'] = df_cliente_display['Valor'].apply(formatar_moeda)
        
        return resumo, perfil, df_cliente_display
    except: return None, "Erro", pd.DataFrame()

# --- FUNÇÃO DE LOGIN ---
def fazer_login(usuario, senha):
    login_busca = usuario
    
    if usuario.lower() == 'rafa' and senha == 'garrafa04':
        login_busca = 'rafa_master'
        
    conn = conectar_banco()
    cursor = conn.cursor()
    cursor.execute("SELECT id, nome, perfil, loja FROM usuarios WHERE LOWER(login) = LOWER(%s) AND senha_hash = %s", (login_busca, senha))
    resultado = cursor.fetchone()
    conn.close()
    return resultado

# --- CONTROLE DE SESSÃO ---
if 'logado' not in st.session_state:
    st.session_state.logado = False
    st.session_state.id_usuario = None
    st.session_state.perfil = ""
    st.session_state.nome_usuario = ""
    st.session_state.loja_usuario = ""

# --- TELA DE LOGIN ---
if not st.session_state.logado:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        try: st.image("logo mais cred nova 2.png", width=250)
        except: pass
        st.title("🔒 Acesso ao Sistema")
        with st.form("form_login"):
            usuario_input = st.text_input("Usuário")
            senha_input = st.text_input("Senha", type="password")
            btn_login = st.form_submit_button("Entrar")
            if btn_login:
                dados_usuario = fazer_login(usuario_input, senha_input)
                if dados_usuario:
                    st.session_state.logado = True
                    st.session_state.id_usuario = dados_usuario[0]
                    st.session_state.nome_usuario = dados_usuario[1]
                    st.session_state.perfil = dados_usuario[2] 
                    st.session_state.loja_usuario = dados_usuario[3]
                    st.rerun()
                else: st.error("Usuário ou senha incorretos.")

# --- TELAS PÓS-LOGIN ---
else:
    st.sidebar.title(f"Bem-vindo(a), {st.session_state.nome_usuario}")
    st.sidebar.write(f"🏢 Loja: **{st.session_state.loja_usuario}**")
    st.sidebar.write(f"👤 Perfil: **{st.session_state.perfil.capitalize()}**")
    st.sidebar.divider()
    
    with st.sidebar.expander("⚙️ Alterar Minha Senha"):
        nova_senha_propria = st.text_input("Nova Senha", type="password", key="senha_propria")
        if st.button("Atualizar Senha", key="btn_senha_propria"):
            if nova_senha_propria.strip() != "":
                conn = conectar_banco(); cursor = conn.cursor()
                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (nova_senha_propria, st.session_state.id_usuario))
                conn.commit(); conn.close()
                st.sidebar.success("✅ Senha atualizada!")

    if st.sidebar.button("Sair do Sistema"):
        st.session_state.logado = False
        st.rerun()

    # -----------------------------------------
    # TELA ADMIN E FECHAMENTO
    # -----------------------------------------
    if st.session_state.perfil in ['admin', 'fechamento']:
        st.title("Painel Gestão e Fechamento 📊")
        
        is_master = (st.session_state.perfil in ['admin', 'fechamento'])
        
        if st.session_state.perfil == 'admin':
            abas = st.tabs([
                "📈 Dashboard", "🔁 Fluxo de Caixa", "⏳ Fechamento", "🔍 Cliente", 
                "📄 Histórico", "👥 Usuários (RH)", "🏦 Contas PIX", "💸 Despesas", "💳 Taxas da Máquina"
            ])
            aba_dash, aba_fluxo, aba_fecha, aba_cliente, aba_hist, aba_usuarios, aba_contas, aba_despesas, aba_taxas = abas
        else: 
            abas = st.tabs(["⏳ Fechamento", "🔍 Cliente", "📄 Histórico", "👥 Usuários (RH)", "💸 Despesas"])
            aba_fecha, aba_cliente, aba_hist, aba_usuarios, aba_despesas = abas
            aba_dash = aba_fluxo = aba_contas = aba_taxas = None

        # --- DASHBOARD ---
        if aba_dash:
            with aba_dash:
                st.subheader("Visão Geral Financeira da Empresa")
                col_f1, col_f2, col_f3 = st.columns(3)
                with col_f1: dash_ini = st.date_input("Analisar a partir de:", datetime.date.today() - datetime.timedelta(days=30), format="DD/MM/YYYY")
                with col_f2: dash_fim = st.date_input("Até:", datetime.date.today(), format="DD/MM/YYYY")
                with col_f3:
                    if is_master: dash_loja = st.selectbox("Filtrar por Loja:", ["Todas"] + LISTA_LOJAS)
                    else: dash_loja = st.selectbox("Filtrar por Loja:", [st.session_state.loja_usuario])
                
                try:
                    conn = conectar_banco()
                    loja_admin = st.session_state.loja_usuario
                    
                    if is_master and dash_loja != "Todas":
                        q_vendas = "SELECT v.data_venda, u.loja, v.valor_venda, v.total_lucro, v.status, v.nome_maquina FROM vendas v JOIN usuarios u ON v.usuario_id = u.id WHERE u.loja = %s AND DATE(v.data_venda) >= %s AND DATE(v.data_venda) <= %s"
                        q_gastos = "SELECT data_gasto, loja, valor_gasto FROM gastos WHERE loja = %s AND DATE(data_gasto) >= %s AND DATE(data_gasto) <= %s"
                        params = (dash_loja, dash_ini, dash_fim)
                    elif is_master:
                        q_vendas = "SELECT v.data_venda, u.loja, v.valor_venda, v.total_lucro, v.status, v.nome_maquina FROM vendas v JOIN usuarios u ON v.usuario_id = u.id WHERE DATE(v.data_venda) >= %s AND DATE(v.data_venda) <= %s"
                        q_gastos = "SELECT data_gasto, loja, valor_gasto FROM gastos WHERE DATE(data_gasto) >= %s AND DATE(data_gasto) <= %s"
                        params = (dash_ini, dash_fim)
                    else:
                        q_vendas = "SELECT v.data_venda, u.loja, v.valor_venda, v.total_lucro, v.status, v.nome_maquina FROM vendas v JOIN usuarios u ON v.usuario_id = u.id WHERE u.loja = %s AND DATE(v.data_venda) >= %s AND DATE(v.data_venda) <= %s"
                        q_gastos = "SELECT data_gasto, loja, valor_gasto FROM gastos WHERE loja = %s AND DATE(data_gasto) >= %s AND DATE(data_gasto) <= %s"
                        params = (loja_admin, dash_ini, dash_fim)
                    
                    df_v = pd.read_sql_query(q_vendas, conn, params=params)
                    df_g = pd.read_sql_query(q_gastos, conn, params=params)
                    conn.close()
                    
                    df_fechadas = df_v[df_v['status'] == 'Fechada']
                    vol_passado = df_fechadas['valor_venda'].sum() if not df_fechadas.empty else 0.0
                    lucro = df_fechadas['total_lucro'].sum() if not df_fechadas.empty else 0.0
                    despesas = df_g['valor_gasto'].sum() if not df_g.empty else 0.0
                    liquido = lucro - despesas
                    qtd_vendas = len(df_fechadas)
                    ticket_medio = (vol_passado / qtd_vendas) if qtd_vendas > 0 else 0.0
                    
                    st.write("### 💰 Entradas e Saídas")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("💳 Total Passado", formatar_moeda(vol_passado))
                    c2.metric("💵 Lucro Bruto", formatar_moeda(lucro))
                    c3.metric("📉 Despesas Pagas", formatar_moeda(despesas))
                    c4.metric("💲 Lucro Líquido", formatar_moeda(liquido), delta=formatar_moeda(liquido), delta_color="normal" if liquido>=0 else "inverse")
                    
                    st.write("### 📊 Resumo de Atendimentos da Equipe")
                    c5, c6, c7, c8 = st.columns(4)
                    c5.metric("✅ Vendas Aprovadas", qtd_vendas)
                    c6.metric("🎯 Média de Valor por Venda", formatar_moeda(ticket_medio))
                    c7.metric("❌ Vendas Recusadas", len(df_v[df_v['status'] == 'Recusada']))
                    c8.metric("⏳ Aguardando Aprovação", len(df_v[df_v['status'] == 'Pendente']))

                    st.divider()

                    if not df_v.empty:
                        col_g1, col_g2 = st.columns(2)
                        with col_g1:
                            df_fechadas['data_venda'] = pd.to_datetime(df_fechadas['data_venda']).dt.date
                            df_trend = df_fechadas.groupby('data_venda')['total_lucro'].sum().reset_index()
                            if not df_trend.empty:
                                fig_linha = px.line(df_trend, x='data_venda', y='total_lucro', title='Evolução de Lucro por Dia', markers=True, color_discrete_sequence=['#2E86C1'])
                                fig_linha.update_layout(xaxis_title="Data", yaxis_title="Lucro (R$)", separators=",.")
                                st.plotly_chart(fig_linha, use_container_width=True)
                        with col_g2:
                            df_status = df_v.groupby('status').size().reset_index(name='Quantidade')
                            cores = {'Fechada': '#28B463', 'Pendente': '#F1C40F', 'Recusada': '#E74C3C'}
                            fig_rosca = px.pie(df_status, values='Quantidade', names='status', title='Taxa de Aprovação vs Recusa', hole=0.4, color='status', color_discrete_map=cores)
                            st.plotly_chart(fig_rosca, use_container_width=True)

                        col_g3, col_g4 = st.columns(2)
                        with col_g3:
                            df_maq = df_fechadas[df_fechadas['nome_maquina'] != 'Múltiplas'].groupby('nome_maquina')['valor_venda'].sum().reset_index()
                            if not df_maq.empty:
                                fig_bar_maq = px.bar(df_maq, x='nome_maquina', y='valor_venda', title='Volume por Máquina (Vendas Simples)', color='nome_maquina')
                                fig_bar_maq.update_layout(yaxis_title="Volume Passado (R$)", separators=",.")
                                st.plotly_chart(fig_bar_maq, use_container_width=True)
                        with col_g4:
                            if is_master and dash_loja == "Todas":
                                df_loja_lucro = df_fechadas.groupby('loja')['total_lucro'].sum().reset_index()
                                if not df_loja_lucro.empty:
                                    fig_bar_loja = px.bar(df_loja_lucro, x='loja', y='total_lucro', title='Lucro Bruto por Loja', color='loja', color_discrete_sequence=px.colors.qualitative.Pastel)
                                    fig_bar_loja.update_layout(yaxis_title="Lucro (R$)", separators=",.")
                                    st.plotly_chart(fig_bar_loja, use_container_width=True)
                except Exception as e: pass

        # --- FLUXO DE CAIXA ---
        if aba_fluxo:
            with aba_fluxo:
                st.subheader("🔁 Extrato de Fluxo de Caixa")
                col_fc1, col_fc2, col_fc3 = st.columns(3)
                with col_fc1: fc_ini = st.date_input("Data Inicial:", datetime.date.today() - datetime.timedelta(days=30), format="DD/MM/YYYY", key="fc_ini")
                with col_fc2: fc_fim = st.date_input("Data Final:", datetime.date.today(), format="DD/MM/YYYY", key="fc_fim")
                with col_fc3:
                    if is_master: fc_loja = st.selectbox("Loja Alvo:", ["Todas"] + LISTA_LOJAS, key="fc_loja")
                    else: fc_loja = st.selectbox("Loja Alvo:", [st.session_state.loja_usuario], key="fc_loja")
                
                try:
                    conn = conectar_banco()
                    if is_master and fc_loja != "Todas":
                        q_entradas = "SELECT DATE(v.data_venda) as data, 'Entrada' as tipo, 'Venda: ' || v.cliente_nome as descricao, v.total_lucro as valor, u.loja FROM vendas v JOIN usuarios u ON v.usuario_id = u.id WHERE v.status = 'Fechada' AND u.loja = %s AND DATE(v.data_venda) >= %s AND DATE(v.data_venda) <= %s"
                        q_saidas = "SELECT DATE(data_gasto) as data, 'Saída' as tipo, descricao_obs as descricao, valor_gasto as valor, loja FROM gastos WHERE loja = %s AND DATE(data_gasto) >= %s AND DATE(data_gasto) <= %s"
                        params = (fc_loja, fc_ini, fc_fim)
                    elif is_master:
                        q_entradas = "SELECT DATE(v.data_venda) as data, 'Entrada' as tipo, 'Venda: ' || v.cliente_nome as descricao, v.total_lucro as valor, u.loja FROM vendas v JOIN usuarios u ON v.usuario_id = u.id WHERE v.status = 'Fechada' AND DATE(v.data_venda) >= %s AND DATE(v.data_venda) <= %s"
                        q_saidas = "SELECT DATE(data_gasto) as data, 'Saída' as tipo, descricao_obs as descricao, valor_gasto as valor, loja FROM gastos WHERE DATE(data_gasto) >= %s AND DATE(data_gasto) <= %s"
                        params = (fc_ini, fc_fim)
                    else:
                        q_entradas = "SELECT DATE(v.data_venda) as data, 'Entrada' as tipo, 'Venda: ' || v.cliente_nome as descricao, v.total_lucro as valor, u.loja FROM vendas v JOIN usuarios u ON v.usuario_id = u.id WHERE v.status = 'Fechada' AND u.loja = %s AND DATE(v.data_venda) >= %s AND DATE(v.data_venda) <= %s"
                        q_saidas = "SELECT DATE(data_gasto) as data, 'Saída' as tipo, descricao_obs as descricao, valor_gasto as valor, loja FROM gastos WHERE loja = %s AND DATE(data_gasto) >= %s AND DATE(data_gasto) <= %s"
                        params = (st.session_state.loja_usuario, fc_ini, fc_fim)
                    
                    df_entradas = pd.read_sql_query(q_entradas, conn, params=params)
                    df_saidas = pd.read_sql_query(q_saidas, conn, params=params)
                    conn.close()
                    
                    df_fluxo = pd.concat([df_entradas, df_saidas], ignore_index=True)
                    
                    if not df_fluxo.empty:
                        df_fluxo['data'] = pd.to_datetime(df_fluxo['data'])
                        df_fluxo = df_fluxo.sort_values(by='data')
                        df_fluxo['Data'] = df_fluxo['data'].dt.strftime('%d/%m/%Y')
                        
                        total_entradas = df_entradas['valor'].sum() if not df_entradas.empty else 0.0
                        total_saidas = df_saidas['valor'].sum() if not df_saidas.empty else 0.0
                        saldo_final = total_entradas - total_saidas
                        
                        st.divider()
                        c1, c2, c3 = st.columns(3)
                        c1.metric("🟢 Total de Entradas", formatar_moeda(total_entradas))
                        c2.metric("🔴 Total de Saídas", formatar_moeda(total_saidas))
                        c3.metric("🔵 Saldo do Período", formatar_moeda(saldo_final), delta=formatar_moeda(saldo_final), delta_color="normal" if saldo_final >= 0 else "inverse")
                        
                        df_grafico = pd.DataFrame({"Categoria": ["Entradas (Receitas)", "Saídas (Despesas)"], "Valor (R$)": [total_entradas, total_saidas], "Cor": ["#28B463", "#E74C3C"]})
                        fig_fc = px.bar(df_grafico, x="Categoria", y="Valor (R$)", color="Categoria", color_discrete_map={"Entradas (Receitas)": "#28B463", "Saídas (Despesas)": "#E74C3C"}, title="Comparativo: O que entrou vs O que saiu")
                        st.plotly_chart(fig_fc, use_container_width=True)
                        
                        st.write("### 📖 Livro Razão (Extrato Detalhado)")
                        df_fluxo_display = df_fluxo[['Data', 'tipo', 'descricao', 'loja', 'valor']].copy()
                        df_fluxo_display.columns = ['Data', 'Tipo', 'Descrição', 'Loja', 'Valor (R$)']
                        df_fluxo_display['Valor (R$)'] = df_fluxo_display['Valor (R$)'].apply(formatar_moeda)
                        st.dataframe(df_fluxo_display, use_container_width=True, hide_index=True)
                    else:
                        st.info("Nenhuma movimentação financeira encontrada neste período.")
                except Exception as e: pass

        # --- FECHAMENTO ---
        with aba_fecha:
            try:
                conn = conectar_banco()
                loja_admin = st.session_state.loja_usuario
                filtro_loja = "" if is_master else f"AND u.loja = '{loja_admin}'"
                
                query_pendentes = f"""
                SELECT v.id as "ID", to_char(v.data_venda, 'DD/MM/YYYY') as "Data", u.loja as "Loja", u.nome as "Atendente",
                       v.cliente_nome as "Cliente", v.chave_pix_cliente as "Chave PIX Destino", v.nome_maquina as "Máquina", v.bandeira_cartao as "Bandeira", v.parcelas as "Parcelas",
                       v.valor_venda as "Valor Total_Raw", v.valor_pix_cliente as "PIX_Raw", v.detalhes_cartoes as "Detalhes JSON", v.detalhes_pagamentos as "Pagamentos JSON",
                       v.bonus_fidelidade as "Bonus_Raw", v.usou_fidelidade as "Usou_Fid"
                FROM vendas v JOIN usuarios u ON v.usuario_id = u.id
                WHERE (v.status = 'Pendente' OR v.status IS NULL) {filtro_loja} ORDER BY v.id DESC
                """
                df_pend = pd.read_sql_query(query_pendentes, conn)
                lista_contas = pd.read_sql_query("SELECT nome_conta FROM contas_pix", conn)['nome_conta'].tolist() or ["Nenhuma conta"]
                
                if df_pend.empty:
                    st.success("Tudo em dia! Nenhuma venda pendente.")
                else:
                    df_pend_display = df_pend.copy()
                    df_pend_display['Valor Total'] = df_pend_display['Valor Total_Raw'].apply(formatar_moeda)
                    df_pend_display['Total a Pagar'] = df_pend_display['PIX_Raw'].apply(formatar_moeda)
                    
                    st.dataframe(df_pend_display.drop(columns=['Valor Total_Raw', 'PIX_Raw', 'Detalhes JSON', 'Pagamentos JSON', 'Bonus_Raw', 'Usou_Fid']), use_container_width=True, hide_index=True)
                    st.divider()
                    
                    venda_id_selecionada = st.selectbox("Selecione o ID da Venda para fechar:", df_pend['ID'].tolist())
                    venda_dados = df_pend[df_pend['ID'] == venda_id_selecionada].iloc[0]
                    venda_raw = float(venda_dados['Valor Total_Raw'])
                    pix_raw = float(venda_dados['PIX_Raw'])
                    bonus_fidelidade = float(venda_dados['Bonus_Raw']) if pd.notna(venda_dados['Bonus_Raw']) else 0.0
                    usou_fid = venda_dados['Usou_Fid']
                    detalhes_json = venda_dados['Detalhes JSON']
                    pagamentos_json = venda_dados['Pagamentos JSON']
                    
                    cursor = conn.cursor()
                    total_taxa = 0.0
                    resumo_html = "### 🧮 Resumo do Cálculo\n"
                    
                    if usou_fid:
                        resumo_html += f"👑 **Cartão Fidelidade:** SIM\n"
                    else:
                        resumo_html += f"👤 **Cartão Fidelidade:** NÃO\n"
                        
                    resumo_html += f"💳 **Valor Total Passado nas Máquinas:** {formatar_moeda(venda_raw)}\n\n"
                    
                    if pd.notna(detalhes_json) and detalhes_json != "":
                        cartoes_usados = json.loads(detalhes_json)
                        resumo_html += "**Desconto das Taxas Individuais:**\n"
                        for c in cartoes_usados:
                            maq_c = c['Máquina']
                            band_c = c['Bandeira']
                            parc_c = c['Parcelas']
                            val_c = float(c['Valor'])
                            
                            cursor.execute("SELECT taxa_percentual FROM taxas_cartoes_v2 WHERE nome_maquina = %s AND bandeira = %s AND parcelas = %s", (maq_c, band_c, parc_c))
                            res = cursor.fetchone()
                            
                            if not res:
                                if band_c in ["Visa", "Mastercard"]:
                                    cursor.execute("SELECT taxa_percentual FROM taxas_cartoes_v2 WHERE nome_maquina = %s AND bandeira = 'Visa/Mastercard' AND parcelas = %s", (maq_c, parc_c))
                                    res = cursor.fetchone()
                                elif band_c in ["Elo", "Hipercard", "American Express", "Outra"]:
                                    cursor.execute("SELECT taxa_percentual FROM taxas_cartoes_v2 WHERE nome_maquina = %s AND bandeira = 'Elo/Hiper/Demais' AND parcelas = %s", (maq_c, parc_c))
                                    res = cursor.fetchone()
                            
                            t_perc = float(res[0]) if res else 0.0
                            t_val = val_c * (t_perc / 100)
                            total_taxa += t_val
                            
                            if t_perc == 0.0:
                                st.warning(f"⚠️ Atenção: Não existe taxa cadastrada no sistema para **{maq_c} + {band_c} + {parc_c}**. Assumimos taxa zero.")
                            resumo_html += f"- {maq_c} ({band_c}) em {parc_c} - {formatar_moeda(val_c)}: Taxa de {t_perc}% = **- {formatar_moeda(t_val)}**\n"
                    else:
                        resumo_html += f"- Erro ao ler cartões\n"

                    if usou_fid:
                        if bonus_fidelidade > 0:
                            resumo_html += f"\n🎁 **Bônus Fidelidade Concedido:** **- {formatar_moeda(bonus_fidelidade)}**\n"
                        else:
                            resumo_html += f"\n🎁 **Bônus Fidelidade:** R$ 0,00 (A compra foi inferior a R$ 500)\n"

                    lucro_automatico = venda_raw - total_taxa - pix_raw
                    
                    resumo_html += f"\n💸 **Formas de Recebimento do Cliente:**\n"
                    if pd.notna(pagamentos_json) and pagamentos_json != "":
                        pagamentos = json.loads(pagamentos_json)
                        for p in pagamentos:
                            if p["Tipo"] == "PIX":
                                resumo_html += f"- PIX (Chave: {p['Chave']}): **- {formatar_moeda(p['Valor'])}**\n"
                            else:
                                resumo_html += f"- {p['Tipo']} ({p['Banco']} | Ag: {p['Agência']} | Cc: {p['Conta']}): **- {formatar_moeda(p['Valor'])}**\n"
                    else:
                        resumo_html += f"- Transferência Legado: **- {formatar_moeda(pix_raw)}**\n"

                    resumo_html += f"\n#### 💰 Lucro Líquido Sugerido: {formatar_moeda(lucro_automatico)}\n"
                    st.write("---")
                    st.markdown(resumo_html)
                    st.write("---")

                    # ✅ MELHORIA 4: Campo para informar o valor recebido via PagSeguro
                    # O sistema usa o valor da PagSeguro para calcular o lucro real automaticamente,
                    # eliminando a necessidade de Bia calcular manualmente.
                    st.write("#### 📲 Valor Recebido via PagSeguro (Opcional)")
                    st.info(
                        "Se você já tem o valor exato que **entrou na conta pela PagSeguro**, informe abaixo. "
                        "O sistema calculará o lucro real automaticamente, sem precisar de cálculo manual."
                    )
                    valor_pagseguro = st.number_input(
                        "Valor que entrou na conta via PagSeguro (R$)",
                        min_value=0.0,
                        value=0.0,
                        step=0.01,
                        help="Informe o valor exato do relatório PagSeguro. Se não souber agora, deixe 0 e use o lucro sugerido acima.",
                        key=f"pagseguro_{venda_id_selecionada}"
                    )

                    # Se informou o valor da PagSeguro, calcula o lucro real com base nele
                    if valor_pagseguro > 0:
                        lucro_calculado_pagseguro = valor_pagseguro - pix_raw
                        st.success(
                            f"✅ **Lucro calculado pela PagSeguro:** {formatar_moeda(lucro_calculado_pagseguro)} "
                            f"(Entrou: {formatar_moeda(valor_pagseguro)} − Pago ao cliente: {formatar_moeda(pix_raw)})"
                        )
                        lucro_para_confirmar = lucro_calculado_pagseguro
                    else:
                        lucro_para_confirmar = lucro_automatico

                    st.write("---")

                    with st.form("form_fechamento", clear_on_submit=False):
                        st.write("#### 🛡️ Confirmação de Segurança")
                        st.info("O sistema calculou o lucro acima com base nas taxas cadastradas. **Se o aplicativo da sua maquininha estiver mostrando um lucro diferente (por causa de mudança de taxa que você não sabia), apague o valor abaixo e digite o Lucro Real.**")
                        
                        acao = st.radio("Ação:", ["✅ Aprovar Venda", "❌ Recusar Venda", "🗑️ Excluir Proposta (Sumiu da Tela)"], horizontal=True)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            conta_saida = st.selectbox("Sua Conta de Saída", lista_contas)
                        with col2:
                            lucro_confirmado = st.number_input("Lucro Real Confirmado (R$) *", value=float(lucro_para_confirmar), step=0.01)
                        with col3:
                            motivo_recusa = st.text_input("Motivo (Só para recusa)")
                        
                        # ✅ MELHORIA 2 e 3: Confirmação antes de processar + reset automático via rerun
                        submitted = st.form_submit_button("Processar Fechamento", type="primary")

                        if submitted:
                            usuario_logado_nome = st.session_state.nome_usuario
                            
                            if acao == "✅ Aprovar Venda":
                                if conta_saida == "Nenhuma conta": 
                                    st.error("Cadastre uma Conta da Empresa primeiro na aba 'Contas PIX'.")
                                else:
                                    # ✅ MELHORIA 2: Diálogo de confirmação antes de aprovar
                                    st.session_state['confirmar_fechamento'] = {
                                        'acao': 'aprovar',
                                        'conta_saida': conta_saida,
                                        'lucro_confirmado': lucro_confirmado,
                                        'venda_id': venda_id_selecionada,
                                        'pix_raw': pix_raw,
                                        'valor_pagseguro': valor_pagseguro,
                                        'usuario': usuario_logado_nome,
                                    }
                                    st.rerun()
                                    
                            elif acao == "❌ Recusar Venda":
                                if motivo_recusa.strip() == "": 
                                    st.error("Para recusar, é obrigatório preencher o Motivo da recusa.")
                                else:
                                    st.session_state['confirmar_fechamento'] = {
                                        'acao': 'recusar',
                                        'motivo_recusa': motivo_recusa,
                                        'venda_id': venda_id_selecionada,
                                        'usuario': usuario_logado_nome,
                                    }
                                    st.rerun()
                                    
                            elif "Excluir" in acao:
                                st.session_state['confirmar_fechamento'] = {
                                    'acao': 'excluir',
                                    'venda_id': venda_id_selecionada,
                                    'usuario': usuario_logado_nome,
                                }
                                st.rerun()

                    # ✅ MELHORIA 2: Exibe diálogo de confirmação FORA do form para evitar duplo clique
                    if 'confirmar_fechamento' in st.session_state:
                        dados_conf = st.session_state['confirmar_fechamento']
                        acao_conf = dados_conf['acao']
                        vid_conf = dados_conf['venda_id']

                        if acao_conf == 'aprovar':
                            st.warning(
                                f"⚠️ **Confirmar aprovação da Venda ID {vid_conf}?**\n\n"
                                f"Lucro a registrar: **{formatar_moeda(dados_conf['lucro_confirmado'])}** | "
                                f"Conta: **{dados_conf['conta_saida']}**"
                            )
                        elif acao_conf == 'recusar':
                            st.warning(f"⚠️ **Confirmar recusa da Venda ID {vid_conf}?**\n\nMotivo: *{dados_conf['motivo_recusa']}*")
                        elif acao_conf == 'excluir':
                            st.warning(f"⚠️ **Confirmar exclusão definitiva da Venda ID {vid_conf}?** Esta ação não pode ser desfeita.")

                        col_sim, col_nao = st.columns(2)
                        with col_sim:
                            if st.button("✅ Sim, confirmar", type="primary", key="btn_confirmar_sim"):
                                try:
                                    conn2 = conectar_banco()
                                    cursor2 = conn2.cursor()

                                    if acao_conf == 'aprovar':
                                        cursor2.execute("""
                                            UPDATE vendas 
                                            SET conta_pix_saida=%s, total_lucro=%s, status='Fechada',
                                                fechado_por=%s, data_fechamento=CURRENT_TIMESTAMP,
                                                valor_pagseguro=%s
                                            WHERE id=%s
                                        """, (
                                            dados_conf['conta_saida'],
                                            dados_conf['lucro_confirmado'],
                                            dados_conf['usuario'],
                                            dados_conf['valor_pagseguro'] if dados_conf['valor_pagseguro'] > 0 else None,
                                            vid_conf
                                        ))
                                        cursor2.execute(
                                            "INSERT INTO entradas_pix (conta_nome, data_entrada, valor, descricao) VALUES (%s, CURRENT_DATE, %s, %s)",
                                            (dados_conf['conta_saida'], -dados_conf['pix_raw'], f"Saída P/ Venda ID {vid_conf}")
                                        )
                                        conn2.commit()
                                        conn2.close()
                                        del st.session_state['confirmar_fechamento']
                                        st.success("✅ Venda aprovada com sucesso!")
                                        time.sleep(1)
                                        # ✅ MELHORIA 3: Reset automático — rerun limpa o formulário
                                        st.rerun()

                                    elif acao_conf == 'recusar':
                                        cursor2.execute("""
                                            UPDATE vendas 
                                            SET status='Recusada', motivo_recusa=%s,
                                                fechado_por=%s, data_fechamento=CURRENT_TIMESTAMP 
                                            WHERE id=%s
                                        """, (dados_conf['motivo_recusa'], dados_conf['usuario'], vid_conf))
                                        conn2.commit()
                                        conn2.close()
                                        del st.session_state['confirmar_fechamento']
                                        st.warning("Venda recusada e devolvida para a atendente corrigir!")
                                        time.sleep(1)
                                        # ✅ MELHORIA 3: Reset automático
                                        st.rerun()

                                    elif acao_conf == 'excluir':
                                        cursor2.execute("DELETE FROM vendas WHERE id = %s", (vid_conf,))
                                        conn2.commit()
                                        conn2.close()
                                        del st.session_state['confirmar_fechamento']
                                        st.success("Proposta EXCLUÍDA definitivamente do sistema!")
                                        time.sleep(1)
                                        # ✅ MELHORIA 3: Reset automático
                                        st.rerun()

                                except Exception as e:
                                    st.error(f"Erro ao processar: {e}")

                        with col_nao:
                            if st.button("❌ Não, cancelar", key="btn_confirmar_nao"):
                                del st.session_state['confirmar_fechamento']
                                st.rerun()

                conn.close()
            except Exception as e: pass

        # --- CLIENTE ---
        with aba_cliente:
            st.subheader("Consultar Histórico do Cliente")
            with st.form("form_busca_cpf"):
                cpf_busca = st.text_input("CPF do Cliente")
                if st.form_submit_button("🔍 Consultar", type="primary") and cpf_busca.strip() != "":
                    resumo, perfil_str, df_hist = consultar_perfil_cliente(cpf_busca.strip())
                    if resumo:
                        st.markdown(f"### Cliente: **{resumo['Nome']}** | Perfil: **{perfil_str}**")
                        st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    else: st.warning("Nenhum registro para este CPF.")

        # --- HISTÓRICO ---
        with aba_hist:
            with st.form("form_filtros"):
                c1, c2, c3, c4 = st.columns(4)
                with c1: d_ini = st.date_input("Início", datetime.date.today() - datetime.timedelta(days=30), format="DD/MM/YYYY")
                with c2: d_fim = st.date_input("Fim", datetime.date.today(), format="DD/MM/YYYY")
                with c3: status_f = st.selectbox("Status", ["Todas", "Fechada", "Pendente", "Recusada"])
                with c4: loja_f = st.selectbox("Loja", ["Todas"] + LISTA_LOJAS) if is_master else st.selectbox("Loja", [st.session_state.loja_usuario])
                if st.form_submit_button("🔍 Buscar"):
                    try:
                        conn = conectar_banco()
                        query_h = """
                            SELECT v.id as "ID", to_char(v.data_venda, 'DD/MM/YYYY') as "Data Venda", 
                                   u.loja as "Loja", u.nome as "Atendente", 
                                   v.cliente_nome as "Cliente", v.cliente_cpf as "CPF", 
                                   v.chave_pix_cliente as "Resumo Contas", v.nome_maquina as "Máquina", 
                                   v.bandeira_cartao as "Bandeira", v.valor_venda as "Valor Passado", 
                                   v.valor_pix_cliente as "Total Pago", 
                                   CASE WHEN v.usou_fidelidade THEN 'Sim' ELSE 'Não' END as "Fidelidade?",
                                   v.bonus_fidelidade as "Bônus", v.conta_pix_saida as "Sua Conta Saída", 
                                   v.total_lucro as "Lucro da Loja", v.status as "Status",
                                   v.fechado_por as "Analisado Por", to_char(v.data_fechamento, 'DD/MM/YYYY HH24:MI') as "Data Análise"
                            FROM vendas v JOIN usuarios u ON v.usuario_id = u.id 
                            WHERE DATE(v.data_venda) >= %s AND DATE(v.data_venda) <= %s
                        """
                        params = [d_ini, d_fim]
                        if loja_f != "Todas": query_h += " AND u.loja = %s"; params.append(loja_f)
                        if status_f != "Todas": query_h += " AND v.status = %s"; params.append(status_f)
                        df_h = pd.read_sql_query(query_h + " ORDER BY v.data_venda DESC, v.id DESC", conn, params=params)
                        conn.close()

                        if not df_h.empty:
                            df_h_disp = df_h.copy()
                            df_h_disp['Valor Passado'] = df_h_disp['Valor Passado'].apply(formatar_moeda)
                            df_h_disp['Total Pago'] = df_h_disp['Total Pago'].apply(formatar_moeda)
                            df_h_disp['Bônus'] = df_h_disp['Bônus'].apply(formatar_moeda)
                            df_h_disp['Lucro da Loja'] = df_h_disp['Lucro da Loja'].apply(formatar_moeda)
                            st.dataframe(df_h_disp, use_container_width=True, hide_index=True)
                            st.download_button("📕 Baixar PDF", gerar_pdf(df_h_disp), "historico.pdf", "application/pdf")
                            st.download_button("📄 Baixar CSV", df_h_disp.to_csv(index=False).encode('utf-8'), "historico.csv", "text/csv")
                        else: st.info("Nenhum dado.")
                    except: pass
            
            # --- PAINEL DE LIMPEZA DE RECUSADAS NO HISTÓRICO ---
            st.divider()
            st.subheader("🗑️ Gerenciar Vendas Recusadas")
            st.write("Se uma proposta foi recusada e ficou travada na tela da atendente (lançada errada ou duplicada), você pode excluí-la definitivamente aqui para limpar a tela da loja.")
            
            try:
                conn = conectar_banco()
                loja_admin = st.session_state.loja_usuario
                filtro_loja_rec = "" if is_master else f"AND u.loja = '{loja_admin}'"
                
                query_recusadas = f"""
                    SELECT v.id, v.cliente_nome, v.valor_venda, u.loja, to_char(v.data_venda, 'DD/MM/YYYY') as data 
                    FROM vendas v JOIN usuarios u ON v.usuario_id = u.id 
                    WHERE v.status = 'Recusada' {filtro_loja_rec} 
                    ORDER BY v.id DESC
                """
                df_recusadas = pd.read_sql_query(query_recusadas, conn)
                
                if not df_recusadas.empty:
                    lista_rec = [f"ID: {row['id']} | {row['cliente_nome']} | R$ {row['valor_venda']} | Loja: {row['loja']} | Data: {row['data']}" for index, row in df_recusadas.iterrows()]
                    with st.form("form_excluir_recusada"):
                        rec_selecionada = st.selectbox("Selecione a venda recusada para excluir permanentemente:", lista_rec)
                        id_rec_alvo = int(rec_selecionada.split("|")[0].replace("ID:", "").strip())
                        
                        if st.form_submit_button("Excluir Proposta Recusada", type="primary"):
                            cursor = conn.cursor()
                            cursor.execute("DELETE FROM vendas WHERE id = %s", (id_rec_alvo,))
                            conn.commit()
                            st.success(f"Venda ID {id_rec_alvo} excluída permanentemente! A tela da atendente foi limpa.")
                            time.sleep(1.5)
                            st.rerun()
                else:
                    st.info("Nenhuma venda com status 'Recusada' no momento.")
                conn.close()
            except Exception as e:
                pass

        # --- USUÁRIOS ---
        with aba_usuarios:
            lojas_permitidas = LISTA_LOJAS if is_master else [st.session_state.loja_usuario]
            
            if st.session_state.perfil == 'fechamento':
                perfis_permitidos = ["atendente"]
                st.info("O seu nível de acesso ('Fechamento') permite criar apenas perfis de Atendente, mas para qualquer Loja.")
            elif st.session_state.perfil == 'admin':
                perfis_permitidos = ["atendente", "fechamento", "admin"]
            else:
                perfis_permitidos = ["atendente"]

            st.subheader("➕ Registrar Novo Funcionário")
            with st.form("form_novo_usuario", clear_on_submit=True):
                st.write("**1. Dados de Acesso e Empresa**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    novo_nome = st.text_input("Nome Completo *")
                    novo_login = st.text_input("Login de Acesso *").lower().strip()
                with col2:
                    nova_senha = st.text_input("Senha *", type="password")
                    nova_loja = st.selectbox("Loja *", lojas_permitidas)
                with col3:
                    novo_perfil = st.selectbox("Nível de Acesso *", perfis_permitidos)
                    novo_salario = st.number_input("Salário Mensal (R$)", min_value=0.0, format="%.2f")
                
                st.write("**2. Dados Pessoais e Contrato**")
                col4, col5, col6 = st.columns(3)
                with col4:
                    # ✅ MELHORIA 1: CPF não obrigatório — campo com label indicando isso
                    novo_cpf = st.text_input("CPF (opcional)")
                    novo_rg = st.text_input("RG")
                with col5:
                    nova_data_inicio = st.date_input("Data de Início", datetime.date.today(), format="DD/MM/YYYY")
                    nova_data_fim = st.date_input("Fim do Contrato (Opcional)", datetime.date.today(), format="DD/MM/YYYY")
                with col6:
                    novo_endereco = st.text_area("Endereço Completo", height=100)
                    
                if st.form_submit_button("Cadastrar Funcionário", type="primary"):
                    if novo_nome and novo_login and nova_senha:
                        if novo_login.lower() in ['rafa_master', 'beu']:
                            st.error("🚨 Este login é reservado pelo sistema. Escolha outro.")
                        else:
                            try:
                                data_fim_db = None if nova_data_fim == nova_data_inicio else nova_data_fim
                                conn = conectar_banco(); cursor = conn.cursor()
                                cursor.execute("""
                                    INSERT INTO usuarios (nome, login, senha_hash, loja, perfil, salario, data_inicio, data_fim, endereco, rg, cpf) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, (novo_nome, novo_login, nova_senha, nova_loja, novo_perfil, novo_salario, nova_data_inicio, data_fim_db, novo_endereco, novo_rg, novo_cpf if novo_cpf.strip() else None))
                                conn.commit(); conn.close()
                                st.success(f"✅ Funcionário cadastrado com sucesso!")
                                time.sleep(1)
                                st.rerun()
                            except: st.error("Erro: Provavelmente este Login já existe.")
                    else: st.error("Preencha todos os campos obrigatórios (*).")
            
            st.divider()
            st.subheader("🛠️ Gerenciar e Editar Equipe")
            
            conn = conectar_banco()
            if is_master: 
                query_rh = """
                    SELECT id as "ID", nome as "Nome", login as "Login", perfil as "Perfil", loja as "Loja",
                           cpf as "CPF", rg as "RG", data_inicio as "Admissão", 
                           data_fim as "Desligamento", salario as "Salário", endereco as "Endereço"
                    FROM usuarios WHERE id != %s AND login != 'rafa_master' ORDER BY loja, nome
                """
                df_equipe = pd.read_sql_query(query_rh, conn, params=(st.session_state.id_usuario,))
            else: 
                query_rh = """
                    SELECT id as "ID", nome as "Nome", login as "Login", perfil as "Perfil", loja as "Loja",
                           cpf as "CPF", rg as "RG", data_inicio as "Admissão", 
                           data_fim as "Desligamento", salario as "Salário", endereco as "Endereço"
                    FROM usuarios WHERE loja = %s AND id != %s AND login != 'rafa_master' ORDER BY nome
                """
                df_equipe = pd.read_sql_query(query_rh, conn, params=(st.session_state.loja_usuario, st.session_state.id_usuario,))
            
            if not df_equipe.empty: 
                df_equipe_disp = df_equipe.copy()
                df_equipe_disp['Salário'] = df_equipe_disp['Salário'].apply(formatar_moeda)
                df_equipe_disp['Admissão'] = pd.to_datetime(df_equipe_disp['Admissão']).dt.strftime('%d/%m/%Y')
                df_equipe_disp['Desligamento'] = pd.to_datetime(df_equipe_disp['Desligamento']).dt.strftime('%d/%m/%Y')
                st.dataframe(df_equipe_disp.fillna("-"), use_container_width=True, hide_index=True)
                
                lista_usuarios_str = [f"{row['ID']} - {row['Nome']} ({row['Login']})" for index, row in df_equipe.iterrows()]
                usuario_selecionado = st.selectbox("Selecione o funcionário que deseja alterar ou excluir:", lista_usuarios_str)
                id_alvo = int(usuario_selecionado.split(" - ")[0])
                
                aba_edit, aba_pass, aba_del = st.tabs(["✏️ Editar Informações", "🔑 Trocar Senha", "🗑️ Excluir Conta"])
                
                cursor = conn.cursor()
                cursor.execute("SELECT nome, login, perfil, loja, salario, data_inicio, data_fim, endereco, rg, cpf FROM usuarios WHERE id = %s", (id_alvo,))
                dados_atuais = cursor.fetchone()
                
                if dados_atuais:
                    c_nome, c_login, c_perfil, c_loja, c_salario, c_dt_ini, c_dt_fim, c_end, c_rg, c_cpf = dados_atuais
                    
                    with aba_edit:
                        with st.form("form_edit_user"):
                            col_e1, col_e2, col_e3 = st.columns(3)
                            with col_e1:
                                edit_nome = st.text_input("Nome", value=c_nome if c_nome else "")
                                edit_login = st.text_input("Login", value=c_login if c_login else "")
                            with col_e2:
                                idx_loja = lojas_permitidas.index(c_loja) if c_loja in lojas_permitidas else 0
                                edit_loja = st.selectbox("Loja Atual", lojas_permitidas, index=idx_loja)
                                idx_perfil = perfis_permitidos.index(c_perfil) if c_perfil in perfis_permitidos else 0
                                edit_perfil = st.selectbox("Perfil de Acesso", perfis_permitidos, index=idx_perfil)
                            with col_e3:
                                edit_salario = st.number_input("Salário Mensal (R$)", value=float(c_salario) if c_salario else 0.0)
                                # ✅ MELHORIA 1: CPF não obrigatório na edição também
                                edit_cpf = st.text_input("CPF (opcional)", value=c_cpf if c_cpf else "")
                                
                            col_e4, col_e5, col_e6 = st.columns(3)
                            with col_e4:
                                edit_rg = st.text_input("RG", value=c_rg if c_rg else "")
                            with col_e5:
                                edit_dt_ini = st.date_input("Admissão", value=c_dt_ini if c_dt_ini else datetime.date.today())
                                edit_dt_fim = st.date_input("Desligamento", value=c_dt_fim if c_dt_fim else datetime.date.today())
                            with col_e6:
                                edit_end = st.text_area("Endereço", value=c_end if c_end else "")
                                
                            if st.form_submit_button("Salvar Edição", type="primary"):
                                if edit_login.lower() in ['rafa_master', 'beu'] and edit_login.lower() != c_login.lower():
                                    st.error("🚨 Nome de login reservado.")
                                else:
                                    try:
                                        d_fim_val = None if edit_dt_ini == edit_dt_fim else edit_dt_fim
                                        cursor.execute("""
                                            UPDATE usuarios 
                                            SET nome=%s, login=%s, loja=%s, perfil=%s, salario=%s, cpf=%s, rg=%s, data_inicio=%s, data_fim=%s, endereco=%s
                                            WHERE id=%s
                                        """, (edit_nome, edit_login, edit_loja, edit_perfil, edit_salario, edit_cpf if edit_cpf.strip() else None, edit_rg, edit_dt_ini, d_fim_val, edit_end, id_alvo))
                                        conn.commit()
                                        st.success("✅ Usuário atualizado com sucesso!")
                                        time.sleep(1)
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Erro ao editar: {e}")
                                        
                    with aba_pass:
                        with st.form("form_senha_user"):
                            nova_senha_alvo = st.text_input("Nova Senha", type="password")
                            if st.form_submit_button("Alterar Senha"):
                                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (nova_senha_alvo, id_alvo))
                                conn.commit()
                                st.success("✅ Senha atualizada!")
                                time.sleep(1)
                                st.rerun()
                                
                    with aba_del:
                        st.warning("⚠️ **Atenção:** Se o funcionário já lançou vendas, o sistema bloqueará a exclusão para não quebrar o Histórico Financeiro. Neste caso, vá na aba 'Trocar Senha' e mude a senha para inativá-lo.")
                        if st.button("🗑️ Tentar Excluir Usuário"):
                            if c_login.lower() in ['rafa_master', 'beu']:
                                st.error("🚨 Você não pode excluir a conta Master!")
                            else:
                                try:
                                    cursor.execute("DELETE FROM usuarios WHERE id = %s", (id_alvo,))
                                    conn.commit()
                                    st.success("✅ Funcionário excluído!")
                                    time.sleep(1)
                                    st.rerun()
                                except psycopg2.errors.ForeignKeyViolation:
                                    conn.rollback()
                                    st.error("🚨 **OPERAÇÃO BLOQUEADA:** Este usuário possui vendas no histórico financeiro! Não é possível apagá-lo. Ao invés disso, troque a senha dele para bloquear o acesso.")
                                except Exception as e:
                                    conn.rollback()
                                    st.error(f"Erro ao excluir: {e}")
            conn.close()

        # --- CONTAS PIX (INVENTÁRIO) ---
        if aba_contas:
            with aba_contas:
                if is_master:
                    st.subheader("🏦 Inventário e Gestão de Contas da Empresa")
                    with st.expander("➕ Nova Conta ou Atualizar Saldo"):
                        with st.form("form_nova_conta"):
                            nova_conta_nome = st.text_input("Nome da Conta *")
                            saldo_inicial = st.number_input("Saldo Inicial Atual (R$)", value=0.0)
                            if st.form_submit_button("Registrar Conta"):
                                try:
                                    conn = conectar_banco(); cursor = conn.cursor()
                                    cursor.execute("INSERT INTO contas_pix (nome_conta, saldo_inicial) VALUES (%s, %s)", (nova_conta_nome, saldo_inicial))
                                    conn.commit(); conn.close(); st.success("Conta registrada!"); st.rerun()
                                except: st.error("Erro ou conta já existe.")
                                
                    st.divider()
                    st.subheader("📊 Saldos das Contas (O que tem hoje)")
                    try:
                        conn = conectar_banco()
                        df_contas = pd.read_sql_query("SELECT nome_conta as \"Conta\", saldo_inicial FROM contas_pix", conn)
                        query_mov = "SELECT conta_nome, sum(valor) as mov_total FROM entradas_pix GROUP BY conta_nome"
                        df_mov = pd.read_sql_query(query_mov, conn)
                        df_final = pd.merge(df_contas, df_mov, left_on="Conta", right_on="conta_nome", how="left").fillna(0)
                        df_final['Saldo Atual (R$)'] = df_final['saldo_inicial'] + df_final['mov_total']
                        df_final_disp = df_final[['Conta', 'saldo_inicial', 'Saldo Atual (R$)']].copy()
                        df_final_disp.columns = ['Conta da Empresa', 'Valor Inicial Padrão', 'Saldo Disponível Hoje']
                        df_final_disp['Valor Inicial Padrão'] = df_final_disp['Valor Inicial Padrão'].apply(formatar_moeda)
                        df_final_disp['Saldo Disponível Hoje'] = df_final_disp['Saldo Disponível Hoje'].apply(formatar_moeda)
                        st.dataframe(df_final_disp, use_container_width=True, hide_index=True)
                        st.write("---")
                        with st.form("form_aporte"):
                            c1, c2 = st.columns(2)
                            with c1: conta_aporte = st.selectbox("Conta", df_contas['Conta'].tolist())
                            with c2: valor_aporte = st.number_input("Valor da Entrada (R$)", min_value=0.01)
                            if st.form_submit_button("Lançar Entrada no Inventário"):
                                cursor = conn.cursor()
                                cursor.execute("INSERT INTO entradas_pix (conta_nome, data_entrada, valor, descricao) VALUES (%s, CURRENT_DATE, %s, 'Aporte Manual')", (conta_aporte, valor_aporte))
                                conn.commit(); conn.close(); st.success("Aporte realizado!"); st.rerun()
                    except: pass
                else: st.warning("Acesso restrito.")

        # --- DESPESAS ---
        if aba_despesas:
            with aba_despesas:
                st.subheader("💸 Lançamento de Gastos")
                with st.form("form_novo_gasto", clear_on_submit=True):
                    c1, c2, c3 = st.columns(3)
                    with c1: dt_g = st.date_input("Data", datetime.date.today(), format="DD/MM/YYYY")
                    with c2: lj_g = st.selectbox("Loja *", LISTA_LOJAS) if is_master else st.selectbox("Loja *", [st.session_state.loja_usuario])
                    with c3: val_g = st.number_input("Valor (R$)", min_value=0.01)
                    desc_g = st.text_input("Descrição *")
                    if st.form_submit_button("Registrar Despesa", type="primary") and desc_g:
                        try:
                            conn = conectar_banco(); cursor = conn.cursor()
                            cursor.execute("INSERT INTO gastos (data_gasto, loja, descricao_obs, valor_gasto) VALUES (%s, %s, %s, %s)", (dt_g, lj_g, desc_g, val_g))
                            conn.commit(); conn.close(); st.success("Registrado!"); st.rerun()
                        except: pass
                
                st.divider()
                st.subheader("📋 Histórico de Despesas")
                try:
                    conn = conectar_banco()
                    if is_master: df_gastos = pd.read_sql_query("SELECT id as \"ID\", to_char(data_gasto, 'DD/MM/YYYY') as \"Data\", loja as \"Loja\", descricao_obs as \"Descrição\", valor_gasto as \"Valor\" FROM gastos ORDER BY data_gasto DESC", conn)
                    else: df_gastos = pd.read_sql_query("SELECT id as \"ID\", to_char(data_gasto, 'DD/MM/YYYY') as \"Data\", loja as \"Loja\", descricao_obs as \"Descrição\", valor_gasto as \"Valor\" FROM gastos WHERE loja = %s ORDER BY data_gasto DESC", conn, params=(st.session_state.loja_usuario,))
                    
                    if not df_gastos.empty:
                        df_gastos_disp = df_gastos.copy()
                        df_gastos_disp['Valor'] = df_gastos_disp['Valor'].apply(formatar_moeda)
                        st.dataframe(df_gastos_disp, use_container_width=True, hide_index=True)
                        with st.form("form_excluir_gasto"):
                            lista_gastos = [f"{row['ID']} - {row['Descrição']} ({row['Valor']})" for index, row in df_gastos_disp.iterrows()]
                            gasto_excluir = st.selectbox("Selecione o registro para excluir:", lista_gastos)
                            id_gasto_alvo = int(gasto_excluir.split(" - ")[0])
                            if st.form_submit_button("Excluir Registro"):
                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM gastos WHERE id = %s", (id_gasto_alvo,))
                                conn.commit()
                                cursor.close()
                                st.success("Despesa excluída com sucesso!")
                                st.rerun()
                    conn.close()
                except: pass

        # --- TAXAS DA MÁQUINA ---
        if aba_taxas:
            with aba_taxas:
                if st.session_state.perfil == 'admin':
                    st.subheader("💳 Painel de Controle de Taxas")
                    
                    with st.expander("➕ Cadastrar Nova Máquina"):
                        with st.form("form_nova_maquina"):
                            nova_maquina_nome = st.text_input("Nome da Nova Máquina (Ex: Stone, Cielo, etc.) *")
                            st.caption("Ao criar, ela receberá as taxas padrão automaticamente. Você poderá editá-las abaixo.")
                            if st.form_submit_button("Adicionar Máquina", type="primary") and nova_maquina_nome.strip():
                                try:
                                    conn = conectar_banco(); cursor = conn.cursor()
                                    for p, t_vm, t_elo in DADOS_TAXAS_PADRAO:
                                        cursor.execute("INSERT INTO taxas_cartoes_v2 (nome_maquina, bandeira, parcelas, taxa_percentual) VALUES (%s, %s, %s, %s) ON CONFLICT (nome_maquina, bandeira, parcelas) DO NOTHING", (nova_maquina_nome.strip(), "Visa/Mastercard", p, t_vm))
                                        cursor.execute("INSERT INTO taxas_cartoes_v2 (nome_maquina, bandeira, parcelas, taxa_percentual) VALUES (%s, %s, %s, %s) ON CONFLICT (nome_maquina, bandeira, parcelas) DO NOTHING", (nova_maquina_nome.strip(), "Elo/Hiper/Demais", p, t_elo))
                                    conn.commit(); conn.close()
                                    st.success(f"Máquina '{nova_maquina_nome}' adicionada com sucesso!")
                                    st.rerun()
                                except Exception as e: st.error(f"Erro ao criar máquina: {e}")

                    lista_maquinas_atualizada = obter_lista_maquinas_rapido()
                    
                    st.write("---")
                    st.write("Selecione a máquina abaixo. A tabela virá preenchida com as taxas atuais. Altere qualquer valor dando dois cliques na célula e depois clique em **Salvar Todas as Taxas**.")
                    
                    maq_selecionada = st.selectbox("Selecione a Máquina para Editar:", lista_maquinas_atualizada)
                    
                    try:
                        conn = conectar_banco()
                        cursor = conn.cursor()
                        cursor.execute("SELECT bandeira, parcelas, taxa_percentual FROM taxas_cartoes_v2 WHERE nome_maquina = %s", (maq_selecionada,))
                        taxas_db = cursor.fetchall()
                        conn.close()
                    except:
                        taxas_db = []

                    df_taxas = pd.DataFrame(DADOS_TAXAS_PADRAO, columns=["Parcela", "Visa/Mastercard", "Elo/Hiper/Demais"])
                    
                    for bandeira, parcela, taxa in taxas_db:
                        idx = df_taxas.index[df_taxas['Parcela'] == parcela].tolist()
                        if idx:
                            if bandeira == "Visa/Mastercard":
                                df_taxas.at[idx[0], "Visa/Mastercard"] = float(taxa)
                            elif bandeira == "Elo/Hiper/Demais":
                                df_taxas.at[idx[0], "Elo/Hiper/Demais"] = float(taxa)
                                
                    df_editado = st.data_editor(
                        df_taxas,
                        column_config={
                            "Parcela": st.column_config.TextColumn("Parcela", disabled=True),
                            "Visa/Mastercard": st.column_config.NumberColumn("Visa/Mastercard (%)", format="%.2f", min_value=0.0, step=0.01),
                            "Elo/Hiper/Demais": st.column_config.NumberColumn("Elo/Hiper/Demais (%)", format="%.2f", min_value=0.0, step=0.01)
                        },
                        hide_index=True,
                        use_container_width=True,
                        key=f"editor_taxas_{maq_selecionada}"
                    )
                    
                    if st.button("💾 Salvar Todas as Taxas", type="primary"):
                        try:
                            conn = conectar_banco()
                            cursor = conn.cursor()
                            
                            for index, row in df_editado.iterrows():
                                p = row["Parcela"]
                                t_vm = float(row["Visa/Mastercard"])
                                t_elo = float(row["Elo/Hiper/Demais"])
                                
                                cursor.execute("""
                                    INSERT INTO taxas_cartoes_v2 (nome_maquina, bandeira, parcelas, taxa_percentual) 
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (nome_maquina, bandeira, parcelas) 
                                    DO UPDATE SET taxa_percentual = EXCLUDED.taxa_percentual;
                                """, (maq_selecionada, "Visa/Mastercard", p, t_vm))
                                
                                cursor.execute("""
                                    INSERT INTO taxas_cartoes_v2 (nome_maquina, bandeira, parcelas, taxa_percentual) 
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (nome_maquina, bandeira, parcelas) 
                                    DO UPDATE SET taxa_percentual = EXCLUDED.taxa_percentual;
                                """, (maq_selecionada, "Elo/Hiper/Demais", p, t_elo))
                                
                            conn.commit()
                            conn.close()
                            st.success(f"✅ Todas as taxas da máquina **{maq_selecionada}** foram salvas/atualizadas no banco de dados!")
                        except Exception as e:
                            st.error(f"Erro ao salvar: {e}")
                else: st.warning("Acesso restrito.")

    # -----------------------------------------
    # TELA DA ATENDENTE
    # -----------------------------------------
    elif st.session_state.perfil == 'atendente':
        st.title(f"Painel da Loja - {st.session_state.loja_usuario}")
        aba_venda, aba_consulta = st.tabs(["📝 Lançar Nova Venda", "🔍 Consultar Cliente (CPF)"])
        
        with aba_venda:
            try:
                conn = conectar_banco()
                df_rec = pd.read_sql_query("SELECT id as \"ID\", to_char(data_venda, 'DD/MM/YYYY') as \"Data\", cliente_nome as \"Cliente\", valor_venda as \"Valor\", motivo_recusa as \"Motivo da Recusa\" FROM vendas WHERE usuario_id = %s AND status = 'Recusada' ORDER BY id DESC", conn, params=(st.session_state.id_usuario,))
                if not df_rec.empty:
                    df_rec_disp = df_rec.copy()
                    df_rec_disp['Valor'] = df_rec_disp['Valor'].apply(formatar_moeda)
                    st.error("⚠️ **Vendas RECUSADAS:** As propostas abaixo foram recusadas pelo caixa central:")
                    st.dataframe(df_rec_disp, use_container_width=True, hide_index=True)
                conn.close()
            except: pass
            
            st.write("### 1. Identificação do Cliente")

            # ✅ MELHORIA 1: CPF não obrigatório para o cliente também
            cliente_cpf_input = st.text_input(
                "CPF do Cliente (opcional)",
                help="O CPF não é obrigatório. Se o cliente não quiser informar, deixe em branco.",
                key="input_cpf_cliente"
            )
            nome_sugerido = ""
            if cliente_cpf_input:
                try:
                    conn = conectar_banco(); cursor = conn.cursor()
                    cursor.execute("SELECT cliente_nome FROM vendas WHERE cliente_cpf = %s ORDER BY id DESC LIMIT 1", (cliente_cpf_input,))
                    resultado_busca = cursor.fetchone()
                    if resultado_busca:
                        nome_sugerido = resultado_busca[0]
                        st.success(f"✅ Cliente encontrado: **{nome_sugerido}**")
                    conn.close()
                except: pass

            cliente_nome = st.text_input("Nome Completo *", value=nome_sugerido, key="input_nome_cliente")
            
            st.write("---")
            st.write("### 2. Cartões e Valores")
            
            col_q1, col_q2 = st.columns(2)
            with col_q1:
                qtd_cartoes = st.number_input("Quantos cartões o cliente vai passar?", min_value=1, max_value=50, value=1, step=1, key="input_qtd_cartoes")
            
            cartoes_inputs = []
            lista_maquinas_venda = ["Selecione..."] + obter_lista_maquinas_rapido()
            
            total_passado_cartoes = 0.0
            
            for i in range(int(qtd_cartoes)):
                st.caption(f"**Cartão {i+1}**")
                c1, c2, c3, c4 = st.columns(4)
                with c1: maq = st.selectbox("Máquina *", lista_maquinas_venda, key=f"maq_{i}")
                with c2: band = st.selectbox("Bandeira *", LISTA_BANDEIRAS_ATENDENTE, key=f"band_{i}")
                with c3: parc = st.selectbox("Parcelas", LISTA_PARCELAS, key=f"parc_{i}")
                with c4: val = st.number_input("Valor Passado no Cartão (R$) *", min_value=0.0, key=f"val_{i}")
                
                total_passado_cartoes += val
                cartoes_inputs.append({"Máquina": maq, "Bandeira": band, "Parcelas": parc, "Valor": val})

            st.write("---")
            st.write("#### 3. Repasse ao Cliente")
            valor_cliente_informado = st.number_input("Valor combinado para transferir ao cliente (R$) *", min_value=0.0, step=10.0, help="O valor líquido que o cliente solicitou/receberá (sem contar o bônus).", key="input_valor_cliente")

            st.write("---")
            st.write("#### 🎁 Bônus Cartão Fidelidade")
            fidelidade_opcao = st.radio(
                "O cliente utilizou o Cartão Fidelidade nesta venda?",
                ["Não", "Sim, somar o Bônus ao valor a transferir", "Sim, já abateu o valor no cartão passado"],
                key="input_fidelidade_opcao"
            )
            
            bonus_concedido = 0.0
            if fidelidade_opcao != "Não":
                bonus_concedido = st.number_input("Digite o Valor do Bônus Concedido (R$) *", min_value=0.0, step=5.0, key="input_bonus_concedido")
            
            valor_alvo_cliente = valor_cliente_informado
            if "somar" in fidelidade_opcao:
                valor_alvo_cliente += bonus_concedido
                
            st.info(f"💳 Total Passado nos Cartões: **{formatar_moeda(total_passado_cartoes)}** | 🏆 Bônus: **{formatar_moeda(bonus_concedido)}**\n\n### 💰 LÍQUIDO A PAGAR AO CLIENTE: {formatar_moeda(valor_alvo_cliente)}")

            st.write("---")
            st.write("### 4. Distribuição nas Contas do Cliente")
            qtd_pagamentos = st.number_input("Em quantas contas ele vai receber esse valor Líquido?", min_value=1, max_value=10, value=1, step=1, key="input_qtd_pagamentos")
            
            pagamentos_inputs = []
            soma_distribuida = 0.0
            
            for i in range(int(qtd_pagamentos)):
                st.markdown(f"**Recebedor {i+1}**")
                col_t, col_v = st.columns(2)
                tipo_pag = col_t.selectbox("Modalidade *", ["PIX", "Conta Corrente", "Conta Poupança"], key=f"tpag_{i}")
                val_pag = col_v.number_input("Valor a Transferir (R$) *", min_value=0.0, key=f"vpag_{i}")
                
                soma_distribuida += val_pag
                chave = banco = agencia = conta = ""
                
                if tipo_pag == "PIX":
                    chave = st.text_input("🔑 Chave PIX *", key=f"chave_{i}")
                    pagamentos_inputs.append({"Tipo": tipo_pag, "Chave": chave, "Valor": val_pag})
                else:
                    col_b, col_ag, col_c = st.columns(3)
                    banco = col_b.text_input("🏦 Banco *", key=f"banco_{i}", placeholder="Ex: Itaú, Caixa...")
                    agencia = col_ag.text_input("🔢 Agência *", key=f"ag_{i}")
                    conta = col_c.text_input("🔢 Conta c/ Dígito *", key=f"conta_{i}")
                    pagamentos_inputs.append({"Tipo": tipo_pag, "Banco": banco, "Agência": agencia, "Conta": conta, "Valor": val_pag})

            st.write("##### ⚖️ Painel de Distribuição")
            falta_distribuir = valor_alvo_cliente - soma_distribuida
            
            if valor_alvo_cliente > 0:
                if falta_distribuir > 0.01:
                    st.warning(f"⚠️ **Atenção:** Ainda falta transferir **{formatar_moeda(falta_distribuir)}** para fechar o valor do cliente.")
                elif falta_distribuir < -0.01:
                    st.error(f"🚨 **Erro:** Você está tentando transferir **{formatar_moeda(abs(falta_distribuir))}** a MAIS do que o sistema calculou!")
                else:
                    st.success(f"✅ **100% Distribuído!** Pode registrar a venda no botão abaixo.")

            st.write("---")
            observacoes = st.text_area("Observações Extras", key="input_observacoes")
            
            # ✅ MELHORIA 2: Confirmação antes de enviar — o botão agora aciona um estado de confirmação
            # em vez de já salvar diretamente. Isso evita cliques duplos e vendas duplicadas.
            if st.button("Registrar Venda (Enviar para o Financeiro)", type="primary"):
                
                cartoes_usados = [c for c in cartoes_inputs if c["Máquina"] != "Selecione..." and c["Bandeira"] != "Selecione..." and c["Valor"] > 0]
                
                pagamentos_validos = True
                for p in pagamentos_inputs:
                    if p["Valor"] <= 0: pagamentos_validos = False
                    if p["Tipo"] == "PIX" and not p.get("Chave", "").strip(): pagamentos_validos = False
                    elif p["Tipo"] != "PIX" and (not p.get("Banco", "").strip() or not p.get("Agência", "").strip() or not p.get("Conta", "").strip()): pagamentos_validos = False

                if cliente_nome.strip() == "":
                    st.error("Preencha o Nome do cliente.")
                elif len(cartoes_usados) < int(qtd_cartoes):
                    st.error("Preencha todos os cartões solicitados (Máquina, Bandeira e Valor).")
                elif not pagamentos_validos:
                    st.error("Preencha todos os dados bancários e garanta que os valores são maiores que zero.")
                elif abs(falta_distribuir) > 0.01:
                    st.error("🚨 Você precisa distribuir exatamente o LÍQUIDO A TRANSFERIR antes de prosseguir.")
                else:
                    # Armazena os dados no session_state para a confirmação
                    st.session_state['confirmar_venda'] = {
                        'cliente_nome': cliente_nome,
                        'cliente_cpf': cliente_cpf_input,
                        'cartoes_usados': cartoes_usados,
                        'pagamentos_inputs': pagamentos_inputs,
                        'total_passado_cartoes': total_passado_cartoes,
                        'soma_distribuida': soma_distribuida,
                        'bonus_concedido': bonus_concedido,
                        'fidelidade_opcao': fidelidade_opcao,
                        'observacoes': observacoes,
                        'qtd_cartoes': int(qtd_cartoes),
                    }
                    st.rerun()

            # ✅ MELHORIA 2: Exibe caixa de confirmação se o botão foi clicado
            if 'confirmar_venda' in st.session_state:
                dados_v = st.session_state['confirmar_venda']
                st.divider()
                st.warning(
                    f"⚠️ **Confirmar envio da venda?**\n\n"
                    f"Cliente: **{dados_v['cliente_nome']}** | "
                    f"Valor passado: **{formatar_moeda(dados_v['total_passado_cartoes'])}** | "
                    f"Líquido ao cliente: **{formatar_moeda(dados_v['soma_distribuida'])}**\n\n"
                    f"Após confirmar, a venda será enviada ao financeiro e a tela será limpa."
                )
                col_ok, col_cancel = st.columns(2)

                with col_ok:
                    if st.button("✅ Sim, enviar para o financeiro", key="btn_confirmar_venda"):
                        try:
                            d = dados_v
                            maq_principal = "Múltiplas" if len(d['cartoes_usados']) > 1 else d['cartoes_usados'][0]["Máquina"]
                            band_principal = "Múltiplas" if len(d['cartoes_usados']) > 1 else d['cartoes_usados'][0]["Bandeira"]
                            detalhes_json = json.dumps(d['cartoes_usados'])
                            detalhes_pag_json = json.dumps(d['pagamentos_inputs'])
                            
                            if len(d['pagamentos_inputs']) > 1:
                                chave_resumo = "Múltiplas Contas"
                            else:
                                p0 = d['pagamentos_inputs'][0]
                                chave_resumo = f"PIX: {p0['Chave']}" if p0["Tipo"] == "PIX" else f"{p0['Banco']} - Ag:{p0['Agência']} Cc:{p0['Conta']}"
                            
                            usou_fid = (d['fidelidade_opcao'] != "Não")
                            conn = conectar_banco(); cursor = conn.cursor()
                            cursor.execute("""
                                INSERT INTO vendas (
                                    usuario_id, cliente_nome, cliente_cpf, chave_pix_cliente, 
                                    nome_maquina, bandeira_cartao, parcelas, valor_venda, 
                                    valor_pix_cliente, observacoes, status, detalhes_cartoes,
                                    detalhes_pagamentos, bonus_fidelidade, usou_fidelidade
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Pendente', %s, %s, %s, %s)
                            """, (
                                st.session_state.id_usuario,
                                d['cliente_nome'],
                                d['cliente_cpf'] if d['cliente_cpf'].strip() else None,
                                chave_resumo, 
                                maq_principal, band_principal,
                                d['cartoes_usados'][0]["Parcelas"],
                                d['total_passado_cartoes'], 
                                d['soma_distribuida'],
                                d['observacoes'],
                                detalhes_json, detalhes_pag_json,
                                d['bonus_concedido'], usou_fid
                            ))
                            conn.commit(); conn.close()
                            
                            st.success("✅ Venda registrada e enviada para o Financeiro com sucesso!")
                            
                            # ✅ MELHORIA 3: Limpa a confirmação e recarrega a tela zerada
                            del st.session_state['confirmar_venda']
                            chaves_manter = ['logado', 'id_usuario', 'perfil', 'nome_usuario', 'loja_usuario']
                            for key in list(st.session_state.keys()):
                                if key not in chaves_manter:
                                    del st.session_state[key]
                            time.sleep(1.5)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Erro ao salvar no banco de dados: {e}")

                with col_cancel:
                    if st.button("❌ Não, voltar e corrigir", key="btn_cancelar_venda"):
                        del st.session_state['confirmar_venda']
                        st.rerun()
                    
        with aba_consulta:
            st.subheader("Verificar Perfil do Cliente")
            with st.form("form_consulta_atendente"):
                cpf_atendente = st.text_input("Digite o CPF do Cliente")
                if st.form_submit_button("Consultar CPF", type="primary") and cpf_atendente.strip() != "":
                    resumo, perfil_str, df_hist = consultar_perfil_cliente(cpf_atendente.strip())
                    if resumo:
                        st.markdown(f"### **{resumo['Nome']}** | {perfil_str}")
                        st.dataframe(df_hist, use_container_width=True, hide_index=True)
                    else: st.info("CPF não encontrado na base de dados.")