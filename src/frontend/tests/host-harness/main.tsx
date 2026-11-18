// Test harness: the production Fleet component with real SSH/Kafka API handlers.
import { createRoot } from 'react-dom/client';
import '@tabler/core/dist/css/tabler.min.css';
import '../../src/app/kafdeck-theme.css';
import { BrokerHostsPanel } from '../../src/app/BrokerHostsPanel';
createRoot(document.getElementById('root')!).render(<BrokerHostsPanel clusterId="test-cluster" />);
