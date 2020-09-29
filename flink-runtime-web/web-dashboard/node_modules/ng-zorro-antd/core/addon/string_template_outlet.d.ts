import { TemplateRef, ViewContainerRef } from '@angular/core';
export declare class NzStringTemplateOutletDirective {
    private viewContainer;
    private defaultTemplate;
    private isTemplate;
    private inputTemplate;
    private inputViewRef;
    private defaultViewRef;
    constructor(viewContainer: ViewContainerRef, defaultTemplate: TemplateRef<void>);
    nzStringTemplateOutlet: string | TemplateRef<void>;
    updateView(): void;
}
