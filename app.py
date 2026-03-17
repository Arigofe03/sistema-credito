import streamlit as st
import psycopg2
import pandas as pd
import datetime
import tempfile
import json
from fpdf import FPDF
import plotly.express as px

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Sistema de Gestão de Vendas", layout="wide")

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
    if valor < 500:
        return 0.0
    elif valor < 1000:
        return 15.0
    else:
        milhares = int(valor // 1000)
        return 20.0 + ((milhares - 1) * 10.0)

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

# --- BUSCA DINÂMICA DE MÁQUINAS ---
def obter_lista_maquinas():
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

# --- INICIALIZAÇÃO AUTOMÁTICA DE TABELAS E COLUNAS ---
def inicializar_banco():
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
        
        # NOVA COLUNA PARA O CARTÃO FIDELIDADE
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS usou_fidelidade BOOLEAN DEFAULT FALSE;")
        
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS fechado_por VARCHAR(100);")
        cursor.execute("ALTER TABLE vendas ADD COLUMN IF NOT EXISTS data_fechamento TIMESTAMP;")
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

inicializar_banco()

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
            # Esconde a coluna booleana pra tabela ficar bonita na tela
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
            
        # Adiciona a coroa de Fidelidade se o cliente já usou alguma vez
        if usou_fid:
            perfil += " | 👑 Fidelidade"
            
        resumo = {"Nome": nome_cliente, "Total de Tentativas": total_operacoes, "Operações Aprovadas": aprovadas, "Operações Recusadas": recusadas, "Volume Movimentado": valor_total}
        df_cliente_display['Valor'] = df_cliente_display['Valor'].apply(formatar_moeda)
        
        return resumo, perfil, df_cliente_display
    except: return None, "Erro", pd.DataFrame()

# --- FUNÇÃO DE LOGIN ---
def fazer_login(usuario, senha):
    conn = conectar_banco()
    cursor = conn.cursor()
    cursor.execute("SELECT id, nome, perfil, loja FROM usuarios WHERE login = %s AND senha_hash = %s", (usuario, senha))
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

        # --- DASHBOARD (SÓ ADMIN) ---
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

        # --- FLUXO DE CAIXA (SÓ ADMIN) ---
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

        # --- FECHAMENTO (COM TRAVA DE LUCRO REAL) ---
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
                        maq_alvo = venda_dados['Máquina']
                        bandeira_alvo = venda_dados['Bandeira']
                        parc_alvo = venda_dados['Parcelas']
                        
                        cursor.execute("SELECT taxa_percentual FROM taxas_cartoes_v2 WHERE nome_maquina = %s AND bandeira = %s AND parcelas = %s", (maq_alvo, bandeira_alvo, parc_alvo))
                        taxa_resultado = cursor.fetchone()
                        
                        if not taxa_resultado:
                            if bandeira_alvo in ["Visa", "Mastercard"]:
                                cursor.execute("SELECT taxa_percentual FROM taxas_cartoes_v2 WHERE nome_maquina = %s AND bandeira = 'Visa/Mastercard' AND parcelas = %s", (maq_alvo, parc_alvo))
                                taxa_resultado = cursor.fetchone()
                            elif bandeira_alvo in ["Elo", "Hipercard", "American Express", "Outra"]:
                                cursor.execute("SELECT taxa_percentual FROM taxas_cartoes_v2 WHERE nome_maquina = %s AND bandeira = 'Elo/Hiper/Demais' AND parcelas = %s", (maq_alvo, parc_alvo))
                                taxa_resultado = cursor.fetchone()
                                
                        t_perc = float(taxa_resultado[0]) if taxa_resultado else 0.0
                        total_taxa = venda_raw * (t_perc / 100)
                        if t_perc == 0.0:
                            st.warning(f"⚠️ Atenção: Não existe taxa cadastrada para **{maq_alvo} + {bandeira_alvo} + {parc_alvo}**. Assumimos taxa zero.")
                        resumo_html += f"- Cartão Único: {maq_alvo} ({bandeira_alvo}) em {parc_alvo}: Taxa de {t_perc}% = **- {formatar_moeda(total_taxa)}**\n"

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
                        resumo_html += f"- Transferência Legado (Chave/Info: {venda_dados['Chave PIX Destino']}): **- {formatar_moeda(pix_raw)}**\n"

                    resumo_html += f"\n#### 💰 Lucro Líquido Sugerido: {formatar_moeda(lucro_automatico)}\n"
                    st.write("---")
                    st.markdown(resumo_html)
                    st.write("---")

                    with st.form("form_fechamento", clear_on_submit=False):
                        st.write("#### 🛡️ Confirmação de Segurança")
                        st.info("O sistema calculou o lucro acima com base nas taxas cadastradas. **Se o aplicativo da sua maquininha estiver mostrando um lucro diferente (por causa de mudança de taxa que você não sabia), apague o valor abaixo e digite o Lucro Real.** O valor salvo será cravado no Histórico e não mudará mais.")
                        
                        acao = st.radio("Ação:", ["✅ Aprovar Venda", "❌ Recusar Venda"], horizontal=True)
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            conta_saida = st.selectbox("Sua Conta de Saída", lista_contas)
                        with col2:
                            lucro_confirmado = st.number_input("Lucro Real Confirmado (R$) *", value=float(lucro_automatico), step=0.01)
                        with col3:
                            motivo_recusa = st.text_input("Motivo (Só para recusa)")
                        
                        if st.form_submit_button("Processar Fechamento", type="primary"):
                            usuario_logado_nome = st.session_state.nome_usuario
                            
                            if acao == "✅ Aprovar Venda":
                                if conta_saida == "Nenhuma conta": 
                                    st.error("Cadastre uma Conta da Empresa primeiro na aba 'Contas PIX'.")
                                else:
                                    cursor.execute("""
                                        UPDATE vendas 
                                        SET conta_pix_saida=%s, total_lucro=%s, status='Fechada', fechado_por=%s, data_fechamento=CURRENT_TIMESTAMP 
                                        WHERE id=%s
                                    """, (conta_saida, lucro_confirmado, usuario_logado_nome, venda_id_selecionada))
                                    
                                    cursor.execute("INSERT INTO entradas_pix (conta_nome, data_entrada, valor, descricao) VALUES (%s, CURRENT_DATE, %s, %s)", (conta_saida, -pix_raw, f"Saída P/ Venda ID {venda_id_selecionada}"))
                                    conn.commit()
                                    st.success("Venda aprovada com o lucro confirmado salvo no histórico!")
                                    st.rerun()
                            else:
                                if motivo_recusa.strip() == "": 
                                    st.error("Para recusar, é obrigatório preencher o Motivo da recusa.")
                                else:
                                    cursor.execute("""
                                        UPDATE vendas 
                                        SET status='Recusada', motivo_recusa=%s, fechado_por=%s, data_fechamento=CURRENT_TIMESTAMP 
                                        WHERE id=%s
                                    """, (motivo_recusa, usuario_logado_nome, venda_id_selecionada))
                                    
                                    conn.commit()
                                    st.warning("Venda recusada e enviada de volta à atendente!")
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
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Tentativas", resumo['Total de Tentativas'])
                        c2.metric("Aprovadas", resumo['Operações Aprovadas'])
                        c3.metric("Recusadas", resumo['Operações Recusadas'])
                        c4.metric("Volume Total", formatar_moeda(resumo['Volume Movimentado']))
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

        # --- USUÁRIOS (MÓDULO RH) ---
        with aba_usuarios:
            lojas_permitidas = LISTA_LOJAS if is_master else [st.session_state.loja_usuario]
            
            if st.session_state.perfil == 'fechamento':
                perfis_permitidos = ["atendente"]
                st.info("O seu nível de acesso ('Fechamento') permite criar apenas perfis de Atendente, mas para qualquer Loja.")
            elif st.session_state.perfil == 'admin':
                perfis_permitidos = ["atendente", "fechamento", "admin"]
            else:
                perfis_permitidos = ["atendente"]

            st.subheader("➕ Registrar Novo Funcionário/Usuário")
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
                    novo_cpf = st.text_input("CPF")
                    novo_rg = st.text_input("RG")
                with col5:
                    nova_data_inicio = st.date_input("Data de Início", datetime.date.today(), format="DD/MM/YYYY")
                    nova_data_fim = st.date_input("Fim do Contrato (Opcional - deixe igual se não houver)", datetime.date.today(), format="DD/MM/YYYY")
                with col6:
                    novo_endereco = st.text_area("Endereço Completo", height=100)
                    
                if st.form_submit_button("Cadastrar Funcionário", type="primary"):
                    if novo_nome and novo_login and nova_senha:
                        try:
                            data_fim_db = None if nova_data_fim == nova_data_inicio else nova_data_fim
                            conn = conectar_banco()
                            cursor = conn.cursor()
                            cursor.execute("""
                                INSERT INTO usuarios (nome, login, senha_hash, loja, perfil, salario, data_inicio, data_fim, endereco, rg, cpf) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (novo_nome, novo_login, nova_senha, nova_loja, novo_perfil, novo_salario, nova_data_inicio, data_fim_db, novo_endereco, novo_rg, novo_cpf))
                            conn.commit(); conn.close()
                            st.success(f"✅ Funcionário '{novo_nome}' cadastrado com sucesso!")
                            st.rerun()
                        except: st.error("Erro: Provavelmente este Login já existe.")
                    else: st.error("Preencha todos os campos obrigatórios (*).")
            
            st.divider()
            st.subheader("🛠️ Lista de Equipe (Cadastro Completo)")
            try:
                conn = conectar_banco()
                if is_master: 
                    query_rh = """
                        SELECT id as "ID", nome as "Nome", login as "Login", perfil as "Perfil", loja as "Loja",
                               cpf as "CPF", rg as "RG", to_char(data_inicio, 'DD/MM/YYYY') as "Admissão", 
                               to_char(data_fim, 'DD/MM/YYYY') as "Desligamento", salario as "Salário", endereco as "Endereço"
                        FROM usuarios WHERE id != %s ORDER BY loja, nome
                    """
                    df_equipe = pd.read_sql_query(query_rh, conn, params=(st.session_state.id_usuario,))
                else: 
                    query_rh = """
                        SELECT id as "ID", nome as "Nome", login as "Login", perfil as "Perfil", loja as "Loja",
                               cpf as "CPF", rg as "RG", to_char(data_inicio, 'DD/MM/YYYY') as "Admissão", 
                               to_char(data_fim, 'DD/MM/YYYY') as "Desligamento", salario as "Salário", endereco as "Endereço"
                        FROM usuarios WHERE loja = %s AND id != %s ORDER BY nome
                    """
                    df_equipe = pd.read_sql_query(query_rh, conn, params=(st.session_state.loja_usuario, st.session_state.id_usuario,))
                
                if not df_equipe.empty: 
                    df_equipe_disp = df_equipe.copy()
                    df_equipe_disp['Salário'] = df_equipe_disp['Salário'].apply(formatar_moeda)
                    st.dataframe(df_equipe_disp, use_container_width=True, hide_index=True)
                    
                    lista_usuarios_str = [f"{row['ID']} - {row['Nome']} ({row['Login']})" for index, row in df_equipe.iterrows()]
                    usuario_selecionado = st.selectbox("Selecione o usuário para alterar senha/excluir:", lista_usuarios_str)
                    id_alvo = int(usuario_selecionado.split(" - ")[0])
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        with st.form("f_senha"):
                            nova_senha_alvo = st.text_input("Nova Senha", type="password")
                            if st.form_submit_button("Mudar Senha") and nova_senha_alvo:
                                cursor = conn.cursor()
                                cursor.execute("UPDATE usuarios SET senha_hash = %s WHERE id = %s", (nova_senha_alvo, id_alvo))
                                conn.commit(); st.success("Senha alterada!"); st.rerun()
                    with c2:
                        with st.form("f_excluir"):
                            if st.form_submit_button("Excluir Usuário e Dados"):
                                cursor = conn.cursor()
                                cursor.execute("DELETE FROM usuarios WHERE id = %s", (id_alvo,))
                                conn.commit(); st.success("Funcionário excluído do sistema!"); st.rerun()
                conn.close()
            except: pass

        # --- CONTAS PIX (INVENTÁRIO) - SÓ ADMIN ---
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
                        st.write("Deseja inserir mais dinheiro manualmente na conta? (Ex: Aporte dos sócios)")
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

        # --- TAXAS DA MÁQUINA (NOVO PADRÃO DINÂMICO) ---
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

                    lista_maquinas_atualizada = obter_lista_maquinas()
                    
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
                    st.error("⚠️ **Vendas RECUSADAS:** Corrija e lance novamente:")
                    st.dataframe(df_rec_disp, use_container_width=True, hide_index=True)
                conn.close()
            except: pass
            
            st.write("### 1. Identificação do Cliente")
            cliente_cpf_input = st.text_input("CPF do Cliente * (Digite e clique fora para buscar o nome)", help="Aperte Enter ou clique fora da caixa após digitar.")
            
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

            st.write("### 2. Cartões Utilizados na Venda")
            qtd_cartoes = st.number_input("Quantos cartões o cliente vai passar nesta venda?", min_value=1, max_value=50, value=1, step=1)

            with st.form("form_nova_venda", clear_on_submit=True):
                st.write("#### Dados de Cadastro")
                c_n, c_p = st.columns(2)
                with c_n: cliente_nome = st.text_input("Nome Completo *", value=nome_sugerido)
                with c_p: chave_pix = st.text_input("Chave PIX do Cliente (Opcional se for banco) *")
                
                st.write("---")
                st.write("#### Lançamento de Cartões")
                cartoes_inputs = []
                
                lista_maquinas_venda = ["Selecione..."] + obter_lista_maquinas()
                
                for i in range(int(qtd_cartoes)):
                    st.caption(f"**Cartão {i+1}**")
                    c1, c2, c3, c4 = st.columns(4)
                    with c1: maq = st.selectbox("Máquina *", lista_maquinas_venda, key=f"maq_{i}")
                    with c2: band = st.selectbox("Bandeira *", LISTA_BANDEIRAS_ATENDENTE, key=f"band_{i}")
                    with c3: parc = st.selectbox("Parcelas", LISTA_PARCELAS, key=f"parc_{i}")
                    with c4: val = st.number_input("Valor Passado no Cartão (R$) *", min_value=0.0, key=f"val_{i}")
                    cartoes_inputs.append({"Máquina": maq, "Bandeira": band, "Parcelas": parc, "Valor": val})

                st.write("---")
                st.write("#### 3. Como o cliente vai receber o dinheiro?")
                
                qtd_pagamentos = st.number_input("Em quantas contas diferentes o cliente vai receber?", min_value=1, max_value=10, value=1, step=1)
                
                pagamentos_inputs = []
                for i in range(int(qtd_pagamentos)):
                    st.caption(f"**Recebedor {i+1}**")
                    col_t, col_v = st.columns(2)
                    with col_t: tipo_pag = st.selectbox("Modalidade *", ["PIX", "Conta Corrente", "Conta Poupança"], key=f"tpag_{i}")
                    with col_v: val_pag = st.number_input("Valor Padrão a Transferir (R$) *", min_value=0.0, key=f"vpag_{i}")
                    
                    if tipo_pag == "PIX":
                        chave = st.text_input("Chave PIX *", key=f"chave_{i}")
                        pagamentos_inputs.append({"Tipo": tipo_pag, "Chave": chave, "Valor": val_pag})
                    else:
                        col_b, col_ag, col_c = st.columns(3)
                        with col_b: banco = st.text_input("Nome do Banco *", key=f"banco_{i}", placeholder="Ex: Itaú, Bradesco...")
                        with col_ag: agencia = st.text_input("Agência *", key=f"ag_{i}")
                        with col_c: conta = st.text_input("Conta c/ Dígito *", key=f"conta_{i}")
                        pagamentos_inputs.append({"Tipo": tipo_pag, "Banco": banco, "Agência": agencia, "Conta": conta, "Valor": val_pag})

                st.write("---")
                st.write("#### 🎁 Bônus Cartão Fidelidade")
                fidelidade_opcao = st.radio(
                    "O cliente utilizou o Cartão Fidelidade nesta venda? (Bônus é dado apenas para valores a partir de R$ 500)",
                    [
                        "Não", 
                        "Sim, somar o Bônus ao valor que o cliente vai receber na conta", 
                        "Sim, o cliente já passou um valor menor na máquina de cartão (Abatido)"
                    ]
                )
                
                st.write("---")
                observacoes = st.text_area("Observações Extras")
                
                if st.form_submit_button("Registrar Venda (Enviar para o Financeiro)", type="primary"):
                    
                    cartoes_usados = [c for c in cartoes_inputs if c["Máquina"] != "Selecione..." and c["Bandeira"] != "Selecione..." and c["Valor"] > 0]
                    valor_total_venda = sum(c["Valor"] for c in cartoes_usados)

                    bonus_calculado = 0.0
                    usou_fid = False
                    
                    if fidelidade_opcao != "Não":
                        usou_fid = True
                        bonus_calculado = calcular_bonus(valor_total_venda)
                        
                        if "somar o Bônus" in fidelidade_opcao:
                            if pagamentos_inputs:
                                pagamentos_inputs[0]["Valor"] += bonus_calculado

                    pagamentos_validos = True
                    for p in pagamentos_inputs:
                        if p["Valor"] <= 0:
                            pagamentos_validos = False
                        if p["Tipo"] == "PIX" and not p.get("Chave", "").strip():
                            pagamentos_validos = False
                        elif p["Tipo"] != "PIX" and (not p.get("Banco", "").strip() or not p.get("Agência", "").strip() or not p.get("Conta", "").strip()):
                            pagamentos_validos = False
                    
                    valor_total_pago = sum(p["Valor"] for p in pagamentos_inputs)

                    if cliente_nome == "" or cliente_cpf_input == "":
                        st.error("Preencha o Nome e CPF do cliente.")
                    elif len(cartoes_usados) < int(qtd_cartoes):
                        st.error("Preencha todos os cartões que solicitou (Máquina, Bandeira e Valor) ou diminua a quantidade.")
                    elif not pagamentos_validos:
                        st.error("Preencha todos os dados das contas de recebimento (Chave, Banco, Agência, etc) e garanta que os valores são maiores que zero.")
                    elif valor_total_pago > valor_total_venda:
                        st.error(f"🚨 O valor total que será pago ao cliente ({formatar_moeda(valor_total_pago)}) não pode ser MAIOR que a soma passada nos cartões ({formatar_moeda(valor_total_venda)})!")
                    else:
                        maq_principal = "Múltiplas" if len(cartoes_usados) > 1 else cartoes_usados[0]["Máquina"]
                        band_principal = "Múltiplas" if len(cartoes_usados) > 1 else cartoes_usados[0]["Bandeira"]
                        
                        detalhes_json = json.dumps(cartoes_usados)
                        detalhes_pag_json = json.dumps(pagamentos_inputs)
                        
                        if len(pagamentos_inputs) > 1:
                            chave_resumo = "Múltiplas Contas"
                        else:
                            p0 = pagamentos_inputs[0]
                            chave_resumo = f"PIX: {p0['Chave']}" if p0["Tipo"] == "PIX" else f"{p0['Banco']} - Ag:{p0['Agência']} Cc:{p0['Conta']}"
                        
                        try:
                            conn = conectar_banco(); cursor = conn.cursor()
                            cursor.execute("""
                                INSERT INTO vendas (
                                    usuario_id, cliente_nome, cliente_cpf, chave_pix_cliente, 
                                    nome_maquina, bandeira_cartao, parcelas, valor_venda, 
                                    valor_pix_cliente, observacoes, status, detalhes_cartoes, detalhes_pagamentos, bonus_fidelidade, usou_fidelidade
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Pendente', %s, %s, %s, %s)
                            """, (st.session_state.id_usuario, cliente_nome, cliente_cpf_input, chave_resumo, 
                                  maq_principal, band_principal, cartoes_usados[0]["Parcelas"], valor_total_venda, 
                                  valor_total_pago, observacoes, detalhes_json, detalhes_pag_json, bonus_calculado, usou_fid))
                            conn.commit(); conn.close()
                            st.success(f"Venda de {formatar_moeda(valor_total_venda)} enviada para análise!")
                        except Exception as e: st.error(f"Erro ao salvar no banco de dados: {e}")
                    
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