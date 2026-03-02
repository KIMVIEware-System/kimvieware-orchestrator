#!/usr/bin/env python3
"""
KIMVIEware Orchestrator - Main Server
Port 8080 - Full browser interface
"""
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.api.enhanced_gateway import app

if __name__ == "__main__":
    import uvicorn
    
    print("""
    ╔════════════════════════════════════════════════════════════════════════════════╗
    ║                  🚀 KIMVIEware Orchestrator Server                            ║
    ║                     Version 4.0.0 - Dashboard Pro                             ║
    ╚════════════════════════════════════════════════════════════════════════════════╝
    
    🌐 Démarrage du serveur...
    📍 Adresse: http://localhost:8080
    
    Onglets disponibles:
      🔵 Soumettre     - Télécharger et analyser un SUT
      📋 Emplois       - Voir tous les emplois en temps réel
      ⚙️ Services      - Vérifier l'état des microservices
      📈 Statistiques  - Voir les statistiques complètes
    
    WebSocket: ws://localhost:8080/ws (mises à jour en temps réel)
    API: http://localhost:8080/api/*
    
    Appuyez sur Ctrl+C pour arrêter le serveur
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info",
        reload=False
    )
