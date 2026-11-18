// Test harness: the production Fleet component with real SSH/Kafka API handlers.
import { createRoot } from 'react-dom/client';
import '@tabler/core/dist/css/tabler.min.css';
import '../../src/app/kafdeck-theme.css';
import { BrokerHostsPanel } from '../../src/app/BrokerHostsPanel';
import { BrokerLogs } from '../../src/features/monitoring/BrokerLogs';
import { BrokerMetrics } from '../../src/features/monitoring/BrokerMetrics';
import { BrokerAgentPanel } from '../../src/app/BrokerAgentPanel';

const clusterId = 'test-cluster';
const page = location.hash === '#agent' ? <BrokerAgentPanel clusterId={clusterId} /> :
  location.hash === '#brokers' ? <BrokerLogs clusterId={clusterId} /> :
  location.hash === '#monitoring' ? <BrokerMetrics clusterId={clusterId} refresh={0} manage={false} seconds={30}
    view="alerts" onInspect={() => {}} inspection={null} logLinks /> : <BrokerHostsPanel clusterId={clusterId} />;
createRoot(document.getElementById('root')!).render(page);
