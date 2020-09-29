import { AfterContentInit, OnChanges, OnDestroy, OnInit, QueryList, SimpleChanges, TemplateRef } from '@angular/core';
import { NgClassType } from '../core/types/ng-class';
import { NzSizeDSType } from '../core/types/size';
import { NzStepComponent } from './nz-step.component';
export declare type NzDirectionType = 'horizontal' | 'vertical';
export declare type NzStatusType = 'wait' | 'process' | 'finish' | 'error';
export declare class NzStepsComponent implements OnChanges, OnInit, OnDestroy, AfterContentInit {
    steps: QueryList<NzStepComponent>;
    nzCurrent: number;
    nzDirection: NzDirectionType;
    nzLabelPlacement: 'horizontal' | 'vertical';
    nzSize: NzSizeDSType;
    nzStartIndex: number;
    nzStatus: NzStatusType;
    nzProgressDot: boolean | TemplateRef<{
        $implicit: TemplateRef<void>;
        status: string;
        index: number;
    }>;
    showProcessDot: boolean;
    customProcessDotTemplate: TemplateRef<{
        $implicit: TemplateRef<void>;
        status: string;
        index: number;
    }>;
    classMap: NgClassType;
    private destroy$;
    ngOnChanges(changes: SimpleChanges): void;
    ngOnInit(): void;
    ngOnDestroy(): void;
    ngAfterContentInit(): void;
    private updateChildrenSteps;
    private setClassMap;
}
