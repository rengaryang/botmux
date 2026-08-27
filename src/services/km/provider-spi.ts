import { z } from 'zod';
import type { KnowledgeCandidateInput, MemoryUpsertInput } from './observation-store.js';

export const KmProviderKindSchema = z.enum([
  'source', 'window-resolver', 'extractor', 'canonicalizer', 'knowledge-router',
  'memory-policy', 'memory-backend', 'retriever', 'reranker', 'prompt-composer', 'exporter',
]);
export type KmProviderKind = z.infer<typeof KmProviderKindSchema>;

export const KmProviderDescriptorSchema = z.object({
  id: z.string().trim().min(1),
  kind: KmProviderKindSchema,
  version: z.string().trim().min(1),
  contractVersion: z.literal(1),
  capabilities: z.array(z.string().trim().min(1)).default([]),
  execution: z.enum(['in-process', 'botmux-cli', 'service']),
  deterministic: z.boolean(),
  supportsShadow: z.boolean(),
  maxBatchSize: z.number().int().positive().max(1_000),
}).strict();
export type KmProviderDescriptor = z.infer<typeof KmProviderDescriptorSchema>;

export const KmPipelineProfileSchema = z.object({
  schemaVersion: z.literal(1),
  profileId: z.string().trim().min(1),
  revision: z.number().int().positive(),
  botAppId: z.string().trim().min(1),
  sourceProvider: z.string().trim().min(1),
  windowProvider: z.string().trim().min(1),
  primaryExtractor: z.string().trim().min(1),
  shadowExtractors: z.array(z.string().trim().min(1)).default([]),
  knowledgeRouter: z.string().trim().min(1),
  memoryPolicy: z.string().trim().min(1),
  memoryBackends: z.object({
    writePolicy: z.enum(['single', 'primary-mirror', 'all', 'shadow-write']),
    primary: z.string().trim().min(1),
    mirrors: z.array(z.string().trim().min(1)).default([]),
  }).strict(),
  injectionMode: z.enum(['off', 'shadow', 'canary', 'active']),
  budgets: z.object({
    sourceBytes: z.number().int().positive().max(1_048_576),
    sourceTokens: z.number().int().positive().max(131_072),
    outputClaims: z.number().int().positive().max(100),
    promptTokens: z.number().int().positive().max(8_000),
  }).strict(),
}).strict().superRefine((profile, ctx) => {
  if (new Set(profile.shadowExtractors).size !== profile.shadowExtractors.length) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['shadowExtractors'], message: 'duplicate shadow extractor' });
  }
  if (profile.shadowExtractors.includes(profile.primaryExtractor)) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['shadowExtractors'], message: 'primary cannot also be shadow' });
  }
});
export type KmPipelineProfile = z.infer<typeof KmPipelineProfileSchema>;

export interface EvidenceSegment { id: string; text: string; start: number; end: number }
export interface EvidenceWindow {
  status: 'resolved' | 'missing' | 'partial' | 'stale' | 'unsupported';
  contentHash?: string;
  segments: EvidenceSegment[];
  warnings: string[];
}

export interface DistillationOutput {
  knowledge: KnowledgeCandidateInput[];
  memories: MemoryUpsertInput[];
  discarded: Array<{ reason: string; sourceRef?: unknown }>;
  warnings: string[];
}

export interface DistillationExtractorProvider {
  descriptor: KmProviderDescriptor;
  extract(input: { window: EvidenceWindow; profile: KmPipelineProfile; sourceEventId: string }): Promise<DistillationOutput>;
}
