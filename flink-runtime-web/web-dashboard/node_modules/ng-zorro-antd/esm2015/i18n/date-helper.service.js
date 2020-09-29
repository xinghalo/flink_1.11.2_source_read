/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { DatePipe } from '@angular/common';
import { Inject, Injectable, Injector, Optional } from '@angular/core';
import fnsFormat from 'date-fns/format';
import fnsGetISOWeek from 'date-fns/get_iso_week';
import fnsParse from 'date-fns/parse';
import { mergeDateConfig, NZ_DATE_CONFIG } from './date-config';
import { NzI18nService } from './nz-i18n.service';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
import * as i2 from "@angular/common";
/**
 * @param {?} injector
 * @param {?} config
 * @param {?} datePipe
 * @return {?}
 */
export function DATE_HELPER_SERVICE_FACTORY(injector, config, datePipe) {
    /** @type {?} */
    const i18n = injector.get(NzI18nService);
    return i18n.getDateLocale()
        ? new DateHelperByDateFns(i18n, config)
        : new DateHelperByDatePipe(i18n, config, datePipe);
}
/**
 * Abstract DateHelperService(Token via Class)
 * Compatibility: compact for original usage by default which using DatePipe
 * @abstract
 */
export class DateHelperService {
    // Indicate whether this service is rely on DatePipe
    /**
     * @param {?} i18n
     * @param {?} config
     */
    constructor(i18n, config) {
        this.i18n = i18n;
        this.config = config;
        this.relyOnDatePipe = this instanceof DateHelperByDatePipe; // Indicate whether this service is rely on DatePipe
        this.config = mergeDateConfig(this.config);
    }
    /**
     * @param {?} text
     * @return {?}
     */
    parseDate(text) {
        if (!text) {
            return;
        }
        return fnsParse(text);
    }
    /**
     * @param {?} text
     * @return {?}
     */
    parseTime(text) {
        if (!text) {
            return;
        }
        return fnsParse(`1970-01-01 ${text}`);
    }
}
DateHelperService.decorators = [
    { type: Injectable, args: [{
                providedIn: 'root',
                useFactory: DATE_HELPER_SERVICE_FACTORY,
                deps: [Injector, [new Optional(), NZ_DATE_CONFIG], DatePipe]
            },] }
];
/** @nocollapse */
DateHelperService.ctorParameters = () => [
    { type: NzI18nService },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [NZ_DATE_CONFIG,] }] }
];
/** @nocollapse */ DateHelperService.ngInjectableDef = i0.defineInjectable({ factory: function DateHelperService_Factory() { return DATE_HELPER_SERVICE_FACTORY(i0.inject(i0.INJECTOR), i0.inject(i1.NZ_DATE_CONFIG, 8), i0.inject(i2.DatePipe)); }, token: i1.DateHelperService, providedIn: "root" });
if (false) {
    /** @type {?} */
    DateHelperService.prototype.relyOnDatePipe;
    /**
     * @type {?}
     * @protected
     */
    DateHelperService.prototype.i18n;
    /**
     * @type {?}
     * @protected
     */
    DateHelperService.prototype.config;
    /**
     * @abstract
     * @param {?} date
     * @return {?}
     */
    DateHelperService.prototype.getISOWeek = function (date) { };
    /**
     * @abstract
     * @return {?}
     */
    DateHelperService.prototype.getFirstDayOfWeek = function () { };
    /**
     * @abstract
     * @param {?} date
     * @param {?} formatStr
     * @return {?}
     */
    DateHelperService.prototype.format = function (date, formatStr) { };
}
/**
 * DateHelper that handles date formats with date-fns
 */
export class DateHelperByDateFns extends DateHelperService {
    /**
     * @param {?} date
     * @return {?}
     */
    getISOWeek(date) {
        return fnsGetISOWeek(date);
    }
    // TODO: Use date-fns's "weekStartsOn" to support different locale when "config.firstDayOfWeek" is null
    // when v2.0 is ready: https://github.com/date-fns/date-fns/blob/v2.0.0-alpha.27/src/locale/en-US/index.js#L23
    /**
     * @return {?}
     */
    getFirstDayOfWeek() {
        return this.config.firstDayOfWeek == null ? 1 : this.config.firstDayOfWeek;
    }
    /**
     * Format a date
     * @see https://date-fns.org/docs/format#description
     * @param {?} date Date
     * @param {?} formatStr format string
     * @return {?}
     */
    format(date, formatStr) {
        return fnsFormat(date, formatStr, { locale: this.i18n.getDateLocale() });
    }
}
/** @nocollapse */ DateHelperByDateFns.ngInjectableDef = i0.defineInjectable({ factory: function DateHelperByDateFns_Factory() { return DATE_HELPER_SERVICE_FACTORY(i0.inject(i0.INJECTOR), i0.inject(i1.NZ_DATE_CONFIG, 8), i0.inject(i2.DatePipe)); }, token: DateHelperByDateFns, providedIn: "root" });
/**
 * DateHelper that handles date formats with angular's date-pipe
 * [BUG] Use DatePipe may cause non-standard week bug, see: https://github.com/NG-ZORRO/ng-zorro-antd/issues/2406
 *
 * @deprecated Maybe removed in next major version due to this serious bug
 */
export class DateHelperByDatePipe extends DateHelperService {
    /**
     * @param {?} i18n
     * @param {?} config
     * @param {?} datePipe
     */
    constructor(i18n, config, datePipe) {
        super(i18n, config);
        this.datePipe = datePipe;
    }
    /**
     * @param {?} date
     * @return {?}
     */
    getISOWeek(date) {
        return +this.format(date, 'w');
    }
    /**
     * @return {?}
     */
    getFirstDayOfWeek() {
        if (this.config.firstDayOfWeek === undefined) {
            /** @type {?} */
            const locale = this.i18n.getLocaleId();
            return locale && ['zh-cn', 'zh-tw'].indexOf(locale.toLowerCase()) > -1 ? 1 : 0;
        }
        return this.config.firstDayOfWeek;
    }
    /**
     * @param {?} date
     * @param {?} formatStr
     * @return {?}
     */
    format(date, formatStr) {
        return date ? (/** @type {?} */ (this.datePipe.transform(date, formatStr, undefined, this.i18n.getLocaleId()))) : '';
    }
    /**
     * Compatible translate the moment-like format pattern to angular's pattern
     * Why? For now, we need to support the existing language formats in AntD, and AntD uses the default temporal syntax.
     *
     * TODO: compare and complete all format patterns
     * Each format docs as below:
     * @link https://momentjs.com/docs/#/displaying/format/ / https://angular.io/api/common/DatePipe#description
     * @param {?} format input format pattern
     * @return {?}
     */
    transCompatFormat(format) {
        return (format &&
            format
                .replace(/Y/g, 'y') // only support y, yy, yyy, yyyy
                .replace(/D/g, 'd')); // d, dd represent of D, DD for momentjs, others are not support
    }
}
/** @nocollapse */
DateHelperByDatePipe.ctorParameters = () => [
    { type: NzI18nService },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [NZ_DATE_CONFIG,] }] },
    { type: DatePipe }
];
/** @nocollapse */ DateHelperByDatePipe.ngInjectableDef = i0.defineInjectable({ factory: function DateHelperByDatePipe_Factory() { return DATE_HELPER_SERVICE_FACTORY(i0.inject(i0.INJECTOR), i0.inject(i1.NZ_DATE_CONFIG, 8), i0.inject(i2.DatePipe)); }, token: DateHelperByDatePipe, providedIn: "root" });
if (false) {
    /**
     * @type {?}
     * @private
     */
    DateHelperByDatePipe.prototype.datePipe;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGF0ZS1oZWxwZXIuc2VydmljZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJpMThuL2RhdGUtaGVscGVyLnNlcnZpY2UudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMzQyxPQUFPLEVBQUUsTUFBTSxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3ZFLE9BQU8sU0FBUyxNQUFNLGlCQUFpQixDQUFDO0FBQ3hDLE9BQU8sYUFBYSxNQUFNLHVCQUF1QixDQUFDO0FBQ2xELE9BQU8sUUFBUSxNQUFNLGdCQUFnQixDQUFDO0FBQ3RDLE9BQU8sRUFBRSxlQUFlLEVBQWdCLGNBQWMsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUM5RSxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sbUJBQW1CLENBQUM7Ozs7Ozs7Ozs7QUFFbEQsTUFBTSxVQUFVLDJCQUEyQixDQUN6QyxRQUFrQixFQUNsQixNQUFvQixFQUNwQixRQUFrQjs7VUFFWixJQUFJLEdBQUcsUUFBUSxDQUFDLEdBQUcsQ0FBQyxhQUFhLENBQUM7SUFDeEMsT0FBTyxJQUFJLENBQUMsYUFBYSxFQUFFO1FBQ3pCLENBQUMsQ0FBQyxJQUFJLG1CQUFtQixDQUFDLElBQUksRUFBRSxNQUFNLENBQUM7UUFDdkMsQ0FBQyxDQUFDLElBQUksb0JBQW9CLENBQUMsSUFBSSxFQUFFLE1BQU0sRUFBRSxRQUFRLENBQUMsQ0FBQztBQUN2RCxDQUFDOzs7Ozs7QUFXRCxNQUFNLE9BQWdCLGlCQUFpQjs7Ozs7O0lBR3JDLFlBQXNCLElBQW1CLEVBQWdELE1BQW9CO1FBQXZGLFNBQUksR0FBSixJQUFJLENBQWU7UUFBZ0QsV0FBTSxHQUFOLE1BQU0sQ0FBYztRQUY3RyxtQkFBYyxHQUFZLElBQUksWUFBWSxvQkFBb0IsQ0FBQyxDQUFDLG9EQUFvRDtR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O0lBWUQsaUJBQWlCLENBQUMsTUFBYztRQUM5QixPQUFPLENBQ0wsTUFBTTtZQUNOLE1BQU07aUJBQ0gsT0FBTyxDQUFDLElBQUksRUFBRSxHQUFHLENBQUMsQ0FBQyxnQ0FBZ0M7aUJBQ25ELE9BQU8sQ0FBQyxJQUFJLEVBQUUsR0FBRyxDQUFDLENBQ3RCLENBQUMsQ0FBQyxnRUFBZ0U7SUFDckUsQ0FBQzs7OztZQXpITSxhQUFhOzRDQWtGakIsUUFBUSxZQUFJLE1BQU0sU0FBQyxjQUFjO1lBeEY3QixRQUFROzs7Ozs7OztJQXlGYix3Q0FBMEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBEYXRlUGlwZSB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQgeyBJbmplY3QsIEluamVjdGFibGUsIEluamVjdG9yLCBPcHRpb25hbCB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IGZuc0Zvcm1hdCBmcm9tICdkYXRlLWZucy9mb3JtYXQnO1xuaW1wb3J0IGZuc0dldElTT1dlZWsgZnJvbSAnZGF0ZS1mbnMvZ2V0X2lzb193ZWVrJztcbmltcG9ydCBmbnNQYXJzZSBmcm9tICdkYXRlLWZucy9wYXJzZSc7XG5pbXBvcnQgeyBtZXJnZURhdGVDb25maWcsIE56RGF0ZUNvbmZpZywgTlpfREFURV9DT05GSUcgfSBmcm9tICcuL2RhdGUtY29uZmlnJztcbmltcG9ydCB7IE56STE4blNlcnZpY2UgfSBmcm9tICcuL256LWkxOG4uc2VydmljZSc7XG5cbmV4cG9ydCBmdW5jdGlvbiBEQVRFX0hFTFBFUl9TRVJWSUNFX0ZBQ1RPUlkoXG4gIGluamVjdG9yOiBJbmplY3RvcixcbiAgY29uZmlnOiBOekRhdGVDb25maWcsXG4gIGRhdGVQaXBlOiBEYXRlUGlwZVxuKTogRGF0ZUhlbHBlclNlcnZpY2Uge1xuICBjb25zdCBpMThuID0gaW5qZWN0b3IuZ2V0KE56STE4blNlcnZpY2UpO1xuICByZXR1cm4gaTE4bi5nZXREYXRlTG9jYWxlKClcbiAgICA/IG5ldyBEYXRlSGVscGVyQnlEYXRlRm5zKGkxOG4sIGNvbmZpZylcbiAgICA6IG5ldyBEYXRlSGVscGVyQnlEYXRlUGlwZShpMThuLCBjb25maWcsIGRhdGVQaXBlKTtcbn1cblxuLyoqXG4gKiBBYnN0cmFjdCBEYXRlSGVscGVyU2VydmljZShUb2tlbiB2aWEgQ2xhc3MpXG4gKiBDb21wYXRpYmlsaXR5OiBjb21wYWN0IGZvciBvcmlnaW5hbCB1c2FnZSBieSBkZWZhdWx0IHdoaWNoIHVzaW5nIERhdGVQaXBlXG4gKi9cbkBJbmplY3RhYmxlKHtcbiAgcHJvdmlkZWRJbjogJ3Jvb3QnLFxuICB1c2VGYWN0b3J5OiBEQVRFX0hFTFBFUl9TRVJWSUNFX0ZBQ1RPUlksXG4gIGRlcHM6IFtJbmplY3RvciwgW25ldyBPcHRpb25hbCgpLCBOWl9EQVRFX0NPTkZJR10sIERhdGVQaXBlXVxufSlcbmV4cG9ydCBhYnN0cmFjdCBjbGFzcyBEYXRlSGVscGVyU2VydmljZSB7XG4gIHJlbHlPbkRhdGVQaXBlOiBib29sZWFuID0gdGhpcyBpbnN0YW5jZW9mIERhdGVIZWxwZXJCeURhdGVQaXBlOyAvLyBJbmRpY2F0ZSB3aGV0aGVyIHRoaXMgc2VydmljZSBpcyByZWx5IG9uIERhdGVQaXBlXG5cbiAgY29uc3RydWN0b3IocHJvdGVjdGVkIGkxOG46IE56STE4blNlcnZpY2UsIEBPcHRpb25hbCgpIEBJbmplY3QoTlpfREFURV9DT05GSUcpIHByb3RlY3RlZCBjb25maWc6IE56RGF0ZUNvbmZpZykge1xuICAgIHRoaXMuY29uZmlnID0gbWVyZ2VEYXRlQ29uZmlnKHRoaXMuY29uZmlnKTtcbiAgfVxuXG4gIGFic3RyYWN0IGdldElTT1dlZWsoZGF0ZTogRGF0ZSk6IG51bWJlcjtcbiAgYWJzdHJhY3QgZ2V0Rmlyc3REYXlPZldlZWsoKTogV2Vla0RheUluZGV4O1xuICBhYnN0cmFjdCBmb3JtYXQoZGF0ZTogRGF0ZSwgZm9ybWF0U3RyOiBzdHJpbmcpOiBzdHJpbmc7XG5cbiAgcGFyc2VEYXRlKHRleHQ6IHN0cmluZyk6IERhdGUgfCB1bmRlZmluZWQge1xuICAgIGlmICghdGV4dCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICByZXR1cm4gZm5zUGFyc2UodGV4dCk7XG4gIH1cblxuICBwYXJzZVRpbWUodGV4dDogc3RyaW5nKTogRGF0ZSB8IHVuZGVmaW5lZCB7XG4gICAgaWYgKCF0ZXh0KSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHJldHVybiBmbnNQYXJzZShgMTk3MC0wMS0wMSAke3RleHR9YCk7XG4gIH1cbn1cblxuLyoqXG4gKiBEYXRlSGVscGVyIHRoYXQgaGFuZGxlcyBkYXRlIGZvcm1hdHMgd2l0aCBkYXRlLWZuc1xuICovXG5leHBvcnQgY2xhc3MgRGF0ZUhlbHBlckJ5RGF0ZUZucyBleHRlbmRzIERhdGVIZWxwZXJTZXJ2aWNlIHtcbiAgZ2V0SVNPV2VlayhkYXRlOiBEYXRlKTogbnVtYmVyIHtcbiAgICByZXR1cm4gZm5zR2V0SVNPV2VlayhkYXRlKTtcbiAgfVxuXG4gIC8vIFRPRE86IFVzZSBkYXRlLWZucydzIFwid2Vla1N0YXJ0c09uXCIgdG8gc3VwcG9ydCBkaWZmZXJlbnQgbG9jYWxlIHdoZW4gXCJjb25maWcuZmlyc3REYXlPZldlZWtcIiBpcyBudWxsXG4gIC8vIHdoZW4gdjIuMCBpcyByZWFkeTogaHR0cHM6Ly9naXRodWIuY29tL2RhdGUtZm5zL2RhdGUtZm5zL2Jsb2IvdjIuMC4wLWFscGhhLjI3L3NyYy9sb2NhbGUvZW4tVVMvaW5kZXguanMjTDIzXG4gIGdldEZpcnN0RGF5T2ZXZWVrKCk6IDAgfCAxIHwgMiB8IDMgfCA0IHwgNSB8IDYge1xuICAgIHJldHVybiB0aGlzLmNvbmZpZy5maXJzdERheU9mV2VlayA9PSBudWxsID8gMSA6IHRoaXMuY29uZmlnLmZpcnN0RGF5T2ZXZWVrO1xuICB9XG5cbiAgLyoqXG4gICAqIEZvcm1hdCBhIGRhdGVcbiAgICogQHNlZSBodHRwczovL2RhdGUtZm5zLm9yZy9kb2NzL2Zvcm1hdCNkZXNjcmlwdGlvblxuICAgKiBAcGFyYW0gZGF0ZSBEYXRlXG4gICAqIEBwYXJhbSBmb3JtYXRTdHIgZm9ybWF0IHN0cmluZ1xuICAgKi9cbiAgZm9ybWF0KGRhdGU6IERhdGUsIGZvcm1hdFN0cjogc3RyaW5nKTogc3RyaW5nIHtcbiAgICByZXR1cm4gZm5zRm9ybWF0KGRhdGUsIGZvcm1hdFN0ciwgeyBsb2NhbGU6IHRoaXMuaTE4bi5nZXREYXRlTG9jYWxlKCkgfSk7XG4gIH1cbn1cblxuLyoqXG4gKiBEYXRlSGVscGVyIHRoYXQgaGFuZGxlcyBkYXRlIGZvcm1hdHMgd2l0aCBhbmd1bGFyJ3MgZGF0ZS1waXBlXG4gKiBbQlVHXSBVc2UgRGF0ZVBpcGUgbWF5IGNhdXNlIG5vbi1zdGFuZGFyZCB3ZWVrIGJ1Zywgc2VlOiBodHRwczovL2dpdGh1Yi5jb20vTkctWk9SUk8vbmctem9ycm8tYW50ZC9pc3N1ZXMvMjQwNlxuICpcbiAqIEBkZXByZWNhdGVkIE1heWJlIHJlbW92ZWQgaW4gbmV4dCBtYWpvciB2ZXJzaW9uIGR1ZSB0byB0aGlzIHNlcmlvdXMgYnVnXG4gKi9cbmV4cG9ydCBjbGFzcyBEYXRlSGVscGVyQnlEYXRlUGlwZSBleHRlbmRzIERhdGVIZWxwZXJTZXJ2aWNlIHtcbiAgY29uc3RydWN0b3IoXG4gICAgaTE4bjogTnpJMThuU2VydmljZSxcbiAgICBAT3B0aW9uYWwoKSBASW5qZWN0KE5aX0RBVEVfQ09ORklHKSBjb25maWc6IE56RGF0ZUNvbmZpZyxcbiAgICBwcml2YXRlIGRhdGVQaXBlOiBEYXRlUGlwZVxuICApIHtcbiAgICBzdXBlcihpMThuLCBjb25maWcpO1xuICB9XG5cbiAgZ2V0SVNPV2VlayhkYXRlOiBEYXRlKTogbnVtYmVyIHtcbiAgICByZXR1cm4gK3RoaXMuZm9ybWF0KGRhdGUsICd3Jyk7XG4gIH1cblxuICBnZXRGaXJzdERheU9mV2VlaygpOiBXZWVrRGF5SW5kZXgge1xuICAgIGlmICh0aGlzLmNvbmZpZy5maXJzdERheU9mV2VlayA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBjb25zdCBsb2NhbGUgPSB0aGlzLmkxOG4uZ2V0TG9jYWxlSWQoKTtcbiAgICAgIHJldHVybiBsb2NhbGUgJiYgWyd6aC1jbicsICd6aC10dyddLmluZGV4T2YobG9jYWxlLnRvTG93ZXJDYXNlKCkpID4gLTEgPyAxIDogMDtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuY29uZmlnLmZpcnN0RGF5T2ZXZWVrO1xuICB9XG5cbiAgZm9ybWF0KGRhdGU6IERhdGUsIGZvcm1hdFN0cjogc3RyaW5nKTogc3RyaW5nIHtcbiAgICByZXR1cm4gZGF0ZSA/IHRoaXMuZGF0ZVBpcGUudHJhbnNmb3JtKGRhdGUsIGZvcm1hdFN0ciwgdW5kZWZpbmVkLCB0aGlzLmkxOG4uZ2V0TG9jYWxlSWQoKSkhIDogJyc7XG4gIH1cblxuICAvKipcbiAgICogQ29tcGF0aWJsZSB0cmFuc2xhdGUgdGhlIG1vbWVudC1saWtlIGZvcm1hdCBwYXR0ZXJuIHRvIGFuZ3VsYXIncyBwYXR0ZXJuXG4gICAqIFdoeT8gRm9yIG5vdywgd2UgbmVlZCB0byBzdXBwb3J0IHRoZSBleGlzdGluZyBsYW5ndWFnZSBmb3JtYXRzIGluIEFudEQsIGFuZCBBbnREIHVzZXMgdGhlIGRlZmF1bHQgdGVtcG9yYWwgc3ludGF4LlxuICAgKlxuICAgKiBUT0RPOiBjb21wYXJlIGFuZCBjb21wbGV0ZSBhbGwgZm9ybWF0IHBhdHRlcm5zXG4gICAqIEVhY2ggZm9ybWF0IGRvY3MgYXMgYmVsb3c6XG4gICAqIEBsaW5rIGh0dHBzOi8vbW9tZW50anMuY29tL2RvY3MvIy9kaXNwbGF5aW5nL2Zvcm1hdC9cbiAgICogQGxpbmsgaHR0cHM6Ly9hbmd1bGFyLmlvL2FwaS9jb21tb24vRGF0ZVBpcGUjZGVzY3JpcHRpb25cbiAgICogQHBhcmFtIGZvcm1hdCBpbnB1dCBmb3JtYXQgcGF0dGVyblxuICAgKi9cbiAgdHJhbnNDb21wYXRGb3JtYXQoZm9ybWF0OiBzdHJpbmcpOiBzdHJpbmcge1xuICAgIHJldHVybiAoXG4gICAgICBmb3JtYXQgJiZcbiAgICAgIGZvcm1hdFxuICAgICAgICAucmVwbGFjZSgvWS9nLCAneScpIC8vIG9ubHkgc3VwcG9ydCB5LCB5eSwgeXl5LCB5eXl5XG4gICAgICAgIC5yZXBsYWNlKC9EL2csICdkJylcbiAgICApOyAvLyBkLCBkZCByZXByZXNlbnQgb2YgRCwgREQgZm9yIG1vbWVudGpzLCBvdGhlcnMgYXJlIG5vdCBzdXBwb3J0XG4gIH1cbn1cblxuLy8vLy8vLy8vLy8vXG5cbmV4cG9ydCB0eXBlIFdlZWtEYXlJbmRleCA9IDAgfCAxIHwgMiB8IDMgfCA0IHwgNSB8IDY7XG4iXX0=