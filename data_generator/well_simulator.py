import numpy as np
import pandas as pd
from scipy.stats import truncnorm
from faker import Faker
import uuid

# Configuración de Semilla para Reproducibilidad (Reis: Idempotencia)
np.random.seed(42)
fake = Faker()

class VacaMuertaSimulator:
    """
    Generador de datos sintéticos hiper-realistas basado en física y estadística.
    Refs: Morozov (2020), Yang & Guo (2019), Granville (2024).
    """
    
    def __init__(self, n_wells=1000):
        self.n_wells = n_wells
        self.df = pd.DataFrame()
        
    def _get_truncated_normal(self, mean, sd, low, upp):
        """
        Genera distribución normal truncada para evitar valores físicos imposibles
        (ej. porosidad negativa). Técnica validada por Granville.
        """
        a, b = (low - mean) / sd, (upp - mean) / sd
        return truncnorm.rvs(a, b, loc=mean, scale=sd, size=self.n_wells)

    def generate_reservoir(self):
        """
        Capa Geológica (Inmutable).
        Usa correlaciones de Morozov: La permeabilidad depende de la porosidad.
        """
        print(f"🌍 Generando Geología para {self.n_wells} pozos...")
        
        # 1. Identificadores (Silverston Pattern)
        self.df['well_id'] = [str(uuid.uuid4()) for _ in range(self.n_wells)]
        self.df['well_name'] = [f"VM-{fake.word().upper()}-{np.random.randint(100,999)}" for _ in range(self.n_wells)]
        
        # 2. Porosidad (Datos reales Vaca Muerta: media ~10-12%)
        self.df['porosity'] = self._get_truncated_normal(mean=0.12, sd=0.03, low=0.04, upp=0.20)
        
        # 3. Permeabilidad (mD) - Correlacionada log-lineal con porosidad (Granville)
        noise = np.random.normal(0, 0.5, self.n_wells)
        self.df['permeability'] = np.exp(self.df['porosity'] * 25 - 3 + noise)
        
        # 4. Presión de Reservorio (bar) - Aumenta con la profundidad
        self.df['tvd_depth'] = self._get_truncated_normal(mean=2900, sd=300, low=2200, upp=3400)
        gradiente_presion = 0.14 # bar/m
        self.df['reservoir_pressure'] = self.df['tvd_depth'] * gradiente_presion

    def generate_design(self):
        """
        Capa de Ingeniería (Controlable).
        """
        print("⚙️ Diseñando fracturas...")
        self.df['lateral_length'] = self._get_truncated_normal(mean=2500, sd=500, low=1000, upp=4000)
        spacing = self._get_truncated_normal(mean=75, sd=15, low=50, upp=120)
        self.df['stage_count'] = (self.df['lateral_length'] / spacing).astype(int)
        self.df['proppant_per_stage'] = self._get_truncated_normal(mean=200, sd=50, low=100, upp=400)
        self.df['fluid_per_stage'] = self._get_truncated_normal(mean=350, sd=80, low=200, upp=600)
        self.df['proppant_intensity'] = (self.df['proppant_per_stage'] * self.df['stage_count']) / self.df['lateral_length']

    def simulate_physics_production(self):
        """
        El Motor Físico (Yang & Guo).
        """
        print("🛢️ Simulando flujo de fluidos (Physics Engine)...")
        k = self.df['permeability']
        h = 50 
        Pr = self.df['reservoir_pressure'] 
        Pwf = 50
        N_fracs = self.df['stage_count']
        conductivity_factor = np.log1p(self.df['proppant_intensity']) 
        
        # Ecuación Simplificada de Producción
        q_base = (k * h * (Pr - Pwf) * N_fracs * conductivity_factor) / 1000
        efficiency = self._get_truncated_normal(mean=0.8, sd=0.15, low=0.3, upp=1.0)
        self.df['initial_production_day1'] = q_base * efficiency
        
        decline_factor = 0.6 
        t = 90
        self.df['cum_oil_3m'] = (self.df['initial_production_day1'] / decline_factor) * (1 - (1 + decline_factor * t)**-0.5) * 30

    def inject_anomalies(self):
        """
        Inyecta fallas (Chaos Monkey).
        """
        print("😈 Inyectando anomalías...")
        mask_fail = (self.df['proppant_per_stage'] > 350) & (self.df['permeability'] < 0.05)
        self.df.loc[mask_fail, 'cum_oil_3m'] *= 0.1 
        self.df.loc[mask_fail, 'failure_type'] = 'SCREENOUT'
        self.df['failure_type'] = self.df['failure_type'].fillna('NONE')
        print(f"   -> {mask_fail.sum()} pozos fallaron por Screen-out.")

    def save_data(self, output_path='shaleops_data.parquet'):
        print(f"💾 Guardando {len(self.df)} registros en {output_path}...")
        self.df.to_parquet(output_path, engine='pyarrow', index=False)
        print("✅ Done.")

if __name__ == "__main__":
    sim = VacaMuertaSimulator(n_wells=5000) 
    sim.generate_reservoir()
    sim.generate_design()
    sim.simulate_physics_production()
    sim.inject_anomalies()
    sim.save_data("bronze_landing_zone.parquet")